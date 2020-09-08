package nl.vroste.zio.kinesis.client
import java.util.UUID

import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.TestUtil.retryOnResourceNotFound
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.zionative.Consumer.InitialPosition
import nl.vroste.zio.kinesis.client.zionative.leaserepository.DynamoDbLeaseRepository
import nl.vroste.zio.kinesis.client.zionative.metrics.{ CloudWatchMetricsPublisher, CloudWatchMetricsPublisherConfig }
import nl.vroste.zio.kinesis.client.zionative._
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.kinesis.exceptions.ShutdownException
import zio._
import zio.clock.Clock
import zio.console._
import zio.duration._
import zio.logging.slf4j.Slf4jLogger
import zio.logging.{ log, Logging }
import zio.stream.{ ZStream, ZTransducer }

/**
 * Runnable used for manually testing various features
 */
object ExampleApp extends zio.App {
  val streamName                      = "zio-test-stream-3" // + java.util.UUID.randomUUID().toString
  val applicationName                 = "testApp-3"         // + java.util.UUID.randomUUID().toString(),
  val nrRecords                       = 40000000
  val produceRate                     = 30000               // Nr records to produce per second
  val nrShards                        = 10
  val reshardFactor                   = 2
  val reshardAfter: Option[Duration]  = Some(10.seconds)
  val enhancedFanout                  = false
  val nrNativeWorkers                 = 1
  val nrKclWorkers                    = 0
  val runtime                         = 10.minute
  val maxRandomWorkerStartDelayMillis = 1 + 0 * 60 * 1000
  val recordProcessingTime: Duration  = 1.millisecond

  val producerSettings = ProducerSettings(
    aggregate = true,
    metricsInterval = 5.seconds,
    bufferSize = 8192 * 8,
    maxParallelRequests = 5
  )

  override def run(
    args: List[String]
  ): ZIO[zio.ZEnv, Nothing, ExitCode] = {

    def worker(id: String) =
      ZStream.unwrapManaged {
        for {
          metrics <- CloudWatchMetricsPublisher.make(applicationName, id)
//          delay   <- zio.random.nextIntBetween(0, maxRandomWorkerStartDelayMillis).map(_.millis).toManaged_
          delay    = 130.seconds
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

    def kclWorker(id: String, requestShutdown: Promise[Nothing, Unit]) =
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

    for {
      _          <- TestUtil.createStreamUnmanaged(streamName, nrShards)
      _          <- TestUtil.getShards(streamName)
      producer   <- produceRecords(streamName, nrRecords).tapError(e => log.error(s"Producer error: ${e}")).fork
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
                 ZIO
                   .service[AdminClient.Service]
                   .flatMap(_.updateShardCount(streamName, Math.ceil(nrShards * reshardFactor).toInt)))
                 .delay(delay)
             )
             .getOrElse(ZIO.unit)
             .fork
      _          <- ZIO.sleep(runtime) raceFirst ZIO.foreachPar_(kclWorkers ++ workers)(_.join) raceFirst producer.join
      _           = println("Interrupting app")
      _          <- producer.interruptFork
      _          <- ZIO.foreachPar(kclWorkers)(_.interrupt)
      _          <- ZIO.foreachPar(workers)(_.interrupt.map { exit =>
             exit.fold(_ => (), nrRecordsProcessed => println(s"Worker processed ${nrRecordsProcessed}"))

           })
    } yield ExitCode.success
  }.foldCauseM(e => log.error(s"Program failed: ${e.prettyPrint}", e).as(ExitCode.failure), ZIO.succeed(_))
    .provideCustomLayer(
      awsEnv // TODO switch back!!
      // localStackEnv
    )

  val loggingLayer = Slf4jLogger.make((_, logEntry) => logEntry, Some(getClass.getName))

  val localStackEnv =
    LocalStackServices.localStackAwsLayer >+>
      (AdminClient.live ++ Client.live ++ DynamoDbLeaseRepository.live).orDie ++
        loggingLayer

  val awsEnv: ZLayer[Clock, Nothing, Clock with Has[KinesisAsyncClient] with Has[CloudWatchAsyncClient] with Has[
    DynamoDbAsyncClient
  ] with Logging with Client with AdminClient with Has[DynamicConsumer.Service] with LeaseRepository with Has[
    CloudWatchMetricsPublisherConfig
  ]] = {
    val httpClient    = HttpClient.make(
      maxConcurrency = 100,
      allowHttp2 = false
//      build = _.proxyConfiguration(ProxyConfiguration.builder().host("localhost").port(9090).build())
//        .buildWithDefaults(
//          AttributeMap.builder.put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, java.lang.Boolean.TRUE).build()
//        )
    )
    val kinesisClient = kinesisAsyncClientLayer()
    val cloudWatch    = cloudWatchAsyncClientLayer()
    val dynamo        = dynamoDbAsyncClientLayer()
    val awsClients    = (httpClient >>> (kinesisClient ++ cloudWatch ++ dynamo)).orDie

    val client      = Client.live.orDie
    val adminClient = AdminClient.live.orDie

    val leaseRepo       = DynamoDbLeaseRepository.live
    val dynamicConsumer = DynamicConsumer.live
    val logging         = loggingLayer

    val metricsPublisherConfig = ZLayer.succeed(CloudWatchMetricsPublisherConfig())

    ZLayer.requires[Clock] >+>
      awsClients ++ loggingLayer >+>
      (client ++ adminClient ++ dynamicConsumer ++ leaseRepo ++ logging ++ metricsPublisherConfig)
  }

  def produceRecords(streamName: String, nrRecords: Int) = {
    val chunkSize = 1000
    Ref
      .make(ProducerMetrics.empty)
      .toManaged_
      .flatMap { totalMetrics =>
        Producer
          .make(
            streamName,
            Serde.asciiString,
            producerSettings,
            metrics =>
              putStrLn(metrics.toString) *>
                totalMetrics
                  .updateAndGet(_ + metrics)
                  .flatMap(m => putStrLn(s"Total metrics: ${m.toString}"))
          )
      }
      .use { producer =>
        ZStream
          .unfoldChunk(0)(i =>
            if ((i + 1) * chunkSize < nrRecords)
              Some((Chunk.fromIterable((i * chunkSize) to ((i + 1) * chunkSize))), i + 1)
            else
              None
          )
          .throttleShape(produceRate / 10, 100.millis)(_.size)
          .map(i => ProducerRecord(s"key$i", "value"))
          .mapChunks(Chunk.single(_))
          .mapMParUnordered(20) { chunk =>
            println(s"Producing chunkie of size ${chunk.size}")
            producer
              .produceChunk(chunk)
              //              .tapBoth(e => log.error(s"error: $e"), _ => log.debug("Produced chunk"))
              .retry(retryOnResourceNotFound && Schedule.recurs(1))
              .tapCause(e => log.error("Producing records chunk failed, will retry", e))
              .retry(Schedule.exponential(1.second))
//              .delay(1.second) // TODO Until we fix throttling bug in Producer
          }
          .runDrain
          .tapCause(e => log.error("Producing records chunk failed", e)) *>
          log.info("Producing records is done!")

      }
  }
}
