package nl.vroste.zio.kinesis.client.dynamicconsumer

import nl.vroste.zio.kinesis.client._
import nl.vroste.zio.kinesis.client.localstack.LocalStackServices
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.zionative.Consumer.InitialPosition
import nl.vroste.zio.kinesis.client.zionative._
import nl.vroste.zio.kinesis.client.zionative.leaserepository.DynamoDbLeaseRepository
import nl.vroste.zio.kinesis.client.zionative.leaserepository.DynamoDbLeaseRepository.TableParameters
import nl.vroste.zio.kinesis.client.zionative.metrics.{ CloudWatchMetricsPublisher, CloudWatchMetricsPublisherConfig }
import software.amazon.awssdk.http.SdkHttpConfigurationOption
import software.amazon.awssdk.utils.AttributeMap
import software.amazon.kinesis.exceptions.ShutdownException
import zio._
import zio.aws.cloudwatch.CloudWatch
import zio.aws.dynamodb
import zio.aws.dynamodb.model.primitives.{ TagKeyString, TagValueString }
import zio.aws.kinesis.Kinesis
import zio.aws.kinesis.model.primitives.{ PositiveIntegerObject, StreamName }
import zio.aws.kinesis.model.{ ScalingType, UpdateShardCountRequest }
import zio.logging.LogFormat
import zio.logging.backend.SLF4J
import zio.stream.{ ZSink, ZStream }

/**
 * Runnable used for manually testing various features
 */
object ExampleApp extends ZIOAppDefault {
  val streamName                      = "zio-test-stream-6" // + java.util.UUID.randomUUID().toString
  val applicationName                 = "testApp-10"        // + java.util.UUID.randomUUID().toString(),
  val nrRecords                       = 300000
  val produceRate                     = 200                 // Nr records to produce per second
  val maxRecordSize                   = 50
  val nrShards                        = 2
  val reshardFactor                   = 2
  val reshardAfter: Option[Duration]  = None                // Some(10.seconds)
  val enhancedFanout                  = false
  val nrNativeWorkers                 = 1
  val nrKclWorkers                    = 0
  val runLength                       = 10.minute
  val maxRandomWorkerStartDelayMillis = 1 + 0 * 60 * 1000
  val recordProcessingTime: Duration  = 1.millisecond

  val producerSettings = ProducerSettings(
    aggregation = Producer.Aggregation.ByPredictedShard(),
    metricsInterval = 5.seconds,
    bufferSize = 8192 * 8,
    maxParallelRequests = 10
  )

  val program: ZIO[
    Kinesis with CloudWatch with CloudWatchMetricsPublisherConfig with DynamicConsumer with LeaseRepository with Scope,
    Throwable,
    ExitCode
  ] =
    for {
      _          <- TestUtil.createStreamUnmanaged(streamName, nrShards)
      _          <- TestUtil.getShards(streamName)
      producer   <- TestUtil
                      .produceRecords(streamName, nrRecords, produceRate, maxRecordSize, producerSettings)
                      .tapError(e => ZIO.logError(s"Producer error: ${e}"))
                      .forkScoped
//      _          <- producer.join
      workers    <- ZIO.foreach((1 to nrNativeWorkers).toList)(id => worker(s"worker${id}").runCount.forkScoped)
      kclWorkers <-
        ZIO.foreach(((1 + nrNativeWorkers) to (nrKclWorkers + nrNativeWorkers)).toList)(id =>
          (for {
            shutdown <- Promise.make[Nothing, Unit]
            fib      <- kclWorker(s"worker${id}", shutdown).runDrain.forkDaemon
            _        <-
              ZIO.never.unit.ensuring(
                ZIO.logWarning(s"Requesting shutdown for worker worker${id}!") *> shutdown.succeed(()) <* fib.join.orDie
              )
          } yield ()).forkScoped
        )
      // Sleep, but abort early if one of our children dies
      _          <- reshardAfter
                      .map(delay =>
                        (ZIO.logInfo("Resharding") *>
                          Kinesis.updateShardCount(
                            UpdateShardCountRequest(
                              StreamName(streamName),
                              PositiveIntegerObject(Math.ceil(nrShards.toDouble * reshardFactor).toInt),
                              ScalingType.UNIFORM_SCALING
                            )
                          ))
                          .delay(delay)
                      )
                      .getOrElse(ZIO.unit)
                      .forkScoped
      _          <- ZIO.sleep(runLength) raceFirst ZIO.foreachParDiscard(kclWorkers ++ workers)(_.join) raceFirst producer.join
      _           = println("Interrupting app")
      _          <- producer.interruptFork
      _          <- ZIO.foreachParDiscard(kclWorkers)(_.interrupt)
      _          <- ZIO.foreachParDiscard(workers)(_.interrupt.map { exit =>
                      exit.fold(_ => (), nrRecordsProcessed => println(s"Worker processed ${nrRecordsProcessed}"))

                    })
    } yield ExitCode.success

  override def run: ZIO[Any with ZIOAppArgs with Scope, Nothing, Any] =
    program
      .foldCauseZIO(
        e => ZIO.logSpan(s"Program failed: ${e.prettyPrint}")(ZIO.logErrorCause(e)),
        ZIO.succeed(_)
      )
      .provideLayer(awsEnv ++ Scope.default)

  def worker(id: String) =
    ZStream.unwrapScoped {
      for {
        metrics <- CloudWatchMetricsPublisher.make(applicationName, id)
        delay   <- zio.Random.nextIntBetween(0, maxRandomWorkerStartDelayMillis).map(_.millis)
        _       <- ZIO.logInfo(s"Waiting ${delay.toMillis} ms to start worker ${id}")
      } yield ZStream.fromZIO(ZIO.sleep(delay)) *> Consumer
        .shardedStream(
          streamName,
          applicationName = applicationName,
          deserializer = Serde.asciiString,
          workerIdentifier = id,
          fetchMode = if (enhancedFanout) FetchMode.EnhancedFanOut() else FetchMode.Polling(batchSize = 1000),
          initialPosition = InitialPosition.TrimHorizon,
          emitDiagnostic = ev =>
            (ev match {
              case ev: DiagnosticEvent.PollComplete =>
                ZIO
                  .logInfo(
                    id + s": PollComplete for ${ev.nrRecords} records of ${ev.shardId}, behind latest: ${ev.behindLatest.toMillis} ms (took ${ev.duration.toMillis} ms)"
                  )
                  .provideLayer(loggingLayer)
              case ev                               => ZIO.logInfo(id + ": " + ev.toString).provideLayer(loggingLayer)
            }) *> metrics.processEvent(ev)
        )
        .flatMapPar(Int.MaxValue) { case (shardID, shardStream, checkpointer) =>
          shardStream
            .tap(r =>
              checkpointer
                .stageOnSuccess(
                  (ZIO.logInfo(s"${id} Processing record $r") *> ZIO
                    .sleep(recordProcessingTime)
                    .when(recordProcessingTime >= 1.millis)).when(false)
                )(r)
            )
            .aggregateAsyncWithin(ZSink.collectAllN[Record[String]](5000), Schedule.fixed(10.second))
            .tap(rs => ZIO.logInfo(s"${id} processed ${rs.size} records on shard ${shardID}").when(true))
            .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
            .tap(_ => checkpointer.checkpoint())
            .catchAll {
              case Right(ShardLeaseLost) =>
                ZStream.fromZIO(ZIO.logInfo(s"${id} Doing checkpoint for ${shardID}: shard lease lost")) *>
                  ZStream.empty

              case Left(e) =>
                ZStream.fromZIO(
                  ZIO.logError(s"${id} shard ${shardID} stream failed with " + e + ": " + e.getStackTrace)
                ) *> ZStream.fail(e)
            }
        }
        .mapConcatChunk(identity(_))
        .ensuring(ZIO.logInfo(s"Worker ${id} stream completed"))
    }

  def kclWorker(
    id: String,
    requestShutdown: Promise[Nothing, Unit]
  ): ZStream[DynamicConsumer with Any, Throwable, DynamicConsumer.Record[
    String
  ]] =
    ZStream.fromZIO(
      zio.Random.nextIntBetween(0, 1000).flatMap(d => ZIO.sleep(d.millis))
    ) *> DynamicConsumer
      .shardedStream(
        streamName,
        applicationName = applicationName,
        deserializer = Serde.asciiString,
        workerIdentifier = id,
        requestShutdown = requestShutdown.await,
        configureKcl = config => if (enhancedFanout) config.withEnhancedFanOut else config.withPolling
      )
      .flatMapPar(Int.MaxValue) { case (shardID, shardStream, checkpointer) =>
        shardStream
          .tap(r =>
            checkpointer
              .stageOnSuccess(ZIO.logInfo(s"${id} Processing record $r").when(false))(r)
          )
          .aggregateAsyncWithin(ZSink.collectAllN[DynamicConsumer.Record[String]](1000), Schedule.fixed(5.minutes))
          .mapConcat(_.lastOption.toList)
          .tap(_ => ZIO.logInfo(s"${id} Checkpointing shard ${shardID}") *> checkpointer.checkpoint)
          .catchAll {
            case _: ShutdownException => // This will be thrown when the shard lease has been stolen
              // Abort the stream when we no longer have the lease

              ZStream.fromZIO(ZIO.logError(s"${id} shard ${shardID} lost")) *> ZStream.empty
            case e                    =>
              ZStream.fromZIO(
                ZIO.logError(s"${id} shard ${shardID} stream failed with" + e + ": " + e.getStackTrace)
              ) *> ZStream.fail(e)
          }
      }

  val loggingLayer: ZLayer[Any, Nothing, Unit] = SLF4J
    .slf4j(
      format = LogFormat.colored
    )

  val localStackEnv =
    LocalStackServices.localStackAwsLayer().orDie >+> (DynamoDbLeaseRepository.live) ++ loggingLayer

  val awsEnv: ZLayer[
    Any,
    Nothing,
    Kinesis
      with CloudWatch
      with dynamodb.DynamoDb
      with DynamicConsumer
      with LeaseRepository
      with CloudWatchMetricsPublisherConfig
  ] = {
    val httpClient = HttpClientBuilder.make(
      maxConcurrency = 100,
      allowHttp2 = true,
      build = _.buildWithDefaults(
        AttributeMap.builder.put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, java.lang.Boolean.TRUE).build
      )
    )

    val kinesisClient = kinesisAsyncClientLayer() // @@ (callLogging)

    val cloudWatch = cloudWatchAsyncClientLayer()
    val dynamo     = dynamoDbAsyncClientLayer()
    val awsClients = (httpClient >+>
      zio.aws.core.config.AwsConfig.default >>>
      (kinesisClient ++ cloudWatch ++ dynamo)).orDie

    val leaseRepo       = DynamoDbLeaseRepository.make(
      DynamoDbLeaseRepository.Settings(
        TableParameters(tags = Seq(dynamodb.model.Tag(TagKeyString("app"), TagValueString("zio-kinesis"))))
      )
    )
    val dynamicConsumer = DynamicConsumer.live

    val metricsPublisherConfig = ZLayer.succeed(CloudWatchMetricsPublisherConfig())

    loggingLayer >+> awsClients >+> (dynamicConsumer ++ leaseRepo ++ metricsPublisherConfig)
  }
}
