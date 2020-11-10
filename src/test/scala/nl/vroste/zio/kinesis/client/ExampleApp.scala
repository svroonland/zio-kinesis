package nl.vroste.zio.kinesis.client

import io.github.vigoo.zioaws.cloudwatch.CloudWatch
import io.github.vigoo.zioaws.core.AwsError
import io.github.vigoo.zioaws.core.aspects.{ AwsCallAspect, Described }
import io.github.vigoo.zioaws.kinesis.Kinesis
import io.github.vigoo.zioaws.kinesis.model.{ ScalingType, UpdateShardCountRequest }
import io.github.vigoo.zioaws.{ dynamodb, kinesis }
import nl.vroste.zio.kinesis.client.localstack.LocalStackServices
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.zionative.Consumer.InitialPosition
import nl.vroste.zio.kinesis.client.zionative._
import nl.vroste.zio.kinesis.client.zionative.leaserepository.DynamoDbLeaseRepository
import nl.vroste.zio.kinesis.client.zionative.metrics.{ CloudWatchMetricsPublisher, CloudWatchMetricsPublisherConfig }
import software.amazon.awssdk.http.SdkHttpConfigurationOption
import software.amazon.awssdk.utils.AttributeMap
import software.amazon.kinesis.exceptions.ShutdownException
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.duration._
import zio.logging.{ log, Logging }
import zio.random.Random
import zio.stream.{ ZStream, ZTransducer }

/**
 * Runnable used for manually testing various features
 */
object ExampleApp extends zio.App {
  val streamName                      = "zio-test-stream-6" // + java.util.UUID.randomUUID().toString
  val applicationName                 = "testApp-10"        // + java.util.UUID.randomUUID().toString(),
  val nrRecords                       = 300000
  val produceRate                     = 200                 // Nr records to produce per second
  val recordSize                      = 50
  val nrShards                        = 2
  val reshardFactor                   = 2
  val reshardAfter: Option[Duration]  = None                // Some(10.seconds)
  val enhancedFanout                  = false
  val nrNativeWorkers                 = 1
  val nrKclWorkers                    = 0
  val runtime                         = 10.minute
  val maxRandomWorkerStartDelayMillis = 1 + 0 * 60 * 1000
  val recordProcessingTime: Duration  = 1.millisecond

  val producerSettings                = ProducerSettings(
    aggregate = true,
    metricsInterval = 5.seconds,
    bufferSize = 8192 * 8,
    maxParallelRequests = 10
  )

  val program: ZIO[Logging with Clock with Blocking with Random with Console with Kinesis with CloudWatch with Has[
    CloudWatchMetricsPublisherConfig
  ] with DynamicConsumer with LeaseRepository, Throwable, ExitCode] = {
    for {
      _          <- TestUtil.createStreamUnmanaged(streamName, nrShards)
      _          <- TestUtil.getShards(streamName)
      producer   <- TestUtil
                    .produceRecords(streamName, nrRecords, produceRate, recordSize, producerSettings)
                    .tapError(e => log.error(s"Producer error: ${e}"))
                    .fork
//      _          <- producer.join
      workers    <- ZIO.foreach(1 to nrNativeWorkers)(id => worker(s"worker${id}").runCount.fork)
      kclWorkers <-
        ZIO.foreach((1 + nrNativeWorkers) to (nrKclWorkers + nrNativeWorkers))(id =>
          (for {
            shutdown <- Promise.make[Nothing, Unit]
            fib      <- kclWorker(s"worker${id}", shutdown).runDrain.forkDaemon
            _        <- ZIO.never.unit.ensuring(
                   log.warn(s"Requesting shutdown for worker worker${id}!") *> shutdown.succeed(()) <* fib.join.orDie
                 )
          } yield ()).fork
        )
      // Sleep, but abort early if one of our children dies
      _          <- reshardAfter
             .map(delay =>
               (log.info("Resharding") *>
                 kinesis.updateShardCount(
                   UpdateShardCountRequest(
                     streamName,
                     Math.ceil(nrShards.toDouble * reshardFactor).toInt,
                     ScalingType.UNIFORM_SCALING
                   )
                 ))
                 .delay(delay)
             )
             .getOrElse(ZIO.unit)
             .fork
      _          <- ZIO.sleep(runtime) raceFirst ZIO.foreachPar_(kclWorkers ++ workers)(_.join) raceFirst producer.join
      _           = println("Interrupting app")
      _          <- producer.interruptFork
      _          <- ZIO.foreachPar_(kclWorkers)(_.interrupt)
      _          <- ZIO.foreachPar_(workers)(_.interrupt.map { exit =>
             exit.fold(_ => (), nrRecordsProcessed => println(s"Worker processed ${nrRecordsProcessed}"))

           })
    } yield ExitCode.success
  }
  override def run(
    args: List[String]
  ): ZIO[zio.ZEnv, Nothing, ExitCode] =
    program
      .foldCauseM(e => log.error(s"Program failed: ${e.prettyPrint}", e).exitCode, ZIO.succeed(_))
      .provideCustomLayer(awsEnv)

  def worker(id: String) =
    ZStream.unwrapManaged {
      for {
        metrics <- CloudWatchMetricsPublisher.make(applicationName, id)
        delay   <- zio.random.nextIntBetween(0, maxRandomWorkerStartDelayMillis).map(_.millis).toManaged_
        _       <- log.info(s"Waiting ${delay.toMillis} ms to start worker ${id}").toManaged_
      } yield ZStream.fromEffect(ZIO.sleep(delay)) *> Consumer
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
                log
                  .info(
                    id + s": PollComplete for ${ev.nrRecords} records of ${ev.shardId}, behind latest: ${ev.behindLatest.toMillis} ms (took ${ev.duration.toMillis} ms)"
                  )
                  .provideLayer(loggingLayer)
              case ev                               => log.info(id + ": " + ev.toString).provideLayer(loggingLayer)
            }) *> metrics.processEvent(ev)
        )
        .flatMapPar(Int.MaxValue) {
          case (shardID, shardStream, checkpointer) =>
            shardStream
              .tap(r =>
                checkpointer
                  .stageOnSuccess(
                    (log.info(s"${id} Processing record $r") *> ZIO
                      .sleep(recordProcessingTime)
                      .when(recordProcessingTime >= 1.millis)).when(false)
                  )(r)
              )
              .aggregateAsyncWithin(ZTransducer.collectAllN(5000), Schedule.fixed(10.second))
              .tap(rs => log.info(s"${id} processed ${rs.size} records on shard ${shardID}").when(true))
              .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
              .tap(_ => checkpointer.checkpoint())
              .catchAll {
                case Right(ShardLeaseLost) =>
                  ZStream.fromEffect(log.info(s"${id} Doing checkpoint for ${shardID}: shard lease lost")) *>
                    ZStream.empty

                case Left(e)               =>
                  ZStream.fromEffect(
                    log.error(s"${id} shard ${shardID} stream failed with " + e + ": " + e.getStackTrace)
                  ) *> ZStream.fail(e)
              }
        }
        .mapConcatChunk(identity)
        .ensuring(log.info(s"Worker ${id} stream completed"))
    }

  def kclWorker(
    id: String,
    requestShutdown: Promise[Nothing, Unit]
  ): ZStream[DynamicConsumer with Blocking with Logging with Clock with Random, Throwable, DynamicConsumer.Record[
    String
  ]] =
    ZStream.fromEffect(
      zio.random.nextIntBetween(0, 1000).flatMap(d => ZIO.sleep(d.millis))
    ) *> DynamicConsumer
      .shardedStream(
        streamName,
        applicationName = applicationName,
        deserializer = Serde.asciiString,
        isEnhancedFanOut = enhancedFanout,
        workerIdentifier = id,
        requestShutdown = requestShutdown.await
      )
      .flatMapPar(Int.MaxValue) {
        case (shardID, shardStream, checkpointer) =>
          shardStream
            .tap(r =>
              checkpointer
                .stageOnSuccess(log.info(s"${id} Processing record $r").when(false))(r)
            )
            .aggregateAsyncWithin(ZTransducer.collectAllN(1000), Schedule.fixed(5.second))
            .mapConcat(_.lastOption.toList)
            .tap(_ => log.info(s"${id} Checkpointing shard ${shardID}") *> checkpointer.checkpoint)
            .catchAll {
              case _: ShutdownException => // This will be thrown when the shard lease has been stolen
                // Abort the stream when we no longer have the lease

                ZStream.fromEffect(log.error(s"${id} shard ${shardID} lost")) *> ZStream.empty
              case e                    =>
                ZStream.fromEffect(
                  log.error(s"${id} shard ${shardID} stream failed with" + e + ": " + e.getStackTrace)
                ) *> ZStream.fail(e)
            }
      }

  val loggingLayer: ZLayer[Any, Nothing, Logging] =
    (Console.live ++ Clock.live) >>> Logging.console() >>> Logging.withRootLoggerName(getClass.getName)

  val localStackEnv =
    LocalStackServices.env.orDie >+> (DynamoDbLeaseRepository.live) ++ loggingLayer

  val awsEnv: ZLayer[
    Any,
    Nothing,
    Kinesis with CloudWatch with dynamodb.DynamoDb with Logging with DynamicConsumer with LeaseRepository with Has[
      CloudWatchMetricsPublisherConfig
    ]
  ] = {
    val httpClient = HttpClientBuilder.make(
      maxConcurrency = 100,
      allowHttp2 = true,
      build = _.buildWithDefaults(
        AttributeMap.builder.put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, java.lang.Boolean.TRUE).build
      )
    )

    val callLogging: AwsCallAspect[Logging] =
      new AwsCallAspect[Logging] {
        override final def apply[R1 <: Logging, A](
          f: ZIO[R1, AwsError, Described[A]]
        ): ZIO[R1, AwsError, Described[A]] =
          f.flatMap {
            case r @ Described(value @ _, description) =>
              log.info(s"Finished [${description.service}/${description.operation}]").as(r)
          }
      }

    val kinesisClient = kinesisAsyncClientLayer() @@ (callLogging)

    val cloudWatch = cloudWatchAsyncClientLayer()
    val dynamo     = dynamoDbAsyncClientLayer()
    val awsClients = ((ZLayer.requires[Logging] ++ httpClient) >>> (kinesisClient ++ cloudWatch ++ dynamo)).orDie

    val leaseRepo       = DynamoDbLeaseRepository.live
    val dynamicConsumer = DynamicConsumer.live
    val logging         = loggingLayer

    val metricsPublisherConfig = ZLayer.succeed(CloudWatchMetricsPublisherConfig())

    logging >+> awsClients >+> (dynamicConsumer ++ leaseRepo ++ metricsPublisherConfig)
  }
}
