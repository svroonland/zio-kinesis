package nl.vroste.zio.kinesis.client
import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.TestUtil.retryOnResourceNotFound
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.zionative.leaserepository.DynamoDbLeaseRepository
import nl.vroste.zio.kinesis.client.zionative.metrics.{ CloudWatchMetricsPublisher, CloudWatchMetricsPublisherConfig }
import nl.vroste.zio.kinesis.client.zionative._
import software.amazon.awssdk.http.SdkHttpConfigurationOption
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.utils.AttributeMap
import software.amazon.kinesis.exceptions.ShutdownException
import zio._
import zio.clock.Clock
import zio.console._
import zio.duration._
import zio.logging.slf4j.Slf4jLogger
import zio.logging.{ log, Logging }
import zio.stream.{ ZStream, ZTransducer }

/**
 * Example app that shows the ZIO-native and KCL workers running in parallel
 */
object ExampleApp extends zio.App {
  val streamName                      = "zio-test-stream-12" // + java.util.UUID.randomUUID().toString
  val nrRecords                       = 2000000
  val nrShards                        = 2
  val enhancedFanout                  = false
  val nrNativeWorkers                 = 1
  val nrKclWorkers                    = 0
  val applicationName                 = "testApp-1"          // + java.util.UUID.randomUUID().toString(),
  val runtime                         = 20.minute
  val maxRandomWorkerStartDelayMillis = 1 + 0 * 30 * 1000    // 20000
  val recordProcessingTime: Duration  = 1.millisecond

  override def run(
    args: List[String]
  ): ZIO[zio.ZEnv, Nothing, ExitCode] = {

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
            fetchMode = if (enhancedFanout) FetchMode.EnhancedFanOut() else FetchMode.Polling(batchSize = 100),
            emitDiagnostic = ev =>
              (ev match {
                case ev: DiagnosticEvent.PollComplete =>
                  log
                    .info(
                      id + s": PollComplete for ${ev.nrRecords} records of ${ev.shardId}, behind latest: ${ev.behindLatest.toMillis} ms (took ${ev.duration.toMillis} ms)"
                    )
                    .provideLayer(loggingEnv)
                case ev                               => log.info(id + ": " + ev.toString).provideLayer(loggingEnv)
              }) *> metrics.processEvent(ev)
          )
          .flatMapPar(Int.MaxValue) {
            case (shardID, shardStream, checkpointer) =>
              shardStream
                .tap(r =>
                  checkpointer
                    .stageOnSuccess(
                      (log.info(s"${id} Processing record $r") *> ZIO.sleep(recordProcessingTime)).when(false)
                    )(r)
                )
                .aggregateAsyncWithin(ZTransducer.collectAllN(1000), Schedule.fixed(5.second))
                .tap(rs => log.info(s"${id} processed ${rs.size} records on shard ${shardID}"))
                .mapConcat(_.lastOption.toList)
                .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
                .tap(_ =>
                  // TODO what if checkpointing fails due to a network error..?
                  checkpointer.checkpoint().catchAll {
                    case Right(ShardLeaseLost) =>
                      ZIO.unit
                    case Left(e)               =>
                      log.error(
                        s"${id} shard ${shardID} stream failed checkpointing with" + e + ": " + e.getStackTrace
                      ) *> ZIO.fail(Left(e))
                  }
                )
                .catchAll {
                  case Right(ShardLeaseLost) =>
                    ZStream.empty

                  case Left(e)               =>
                    ZStream.fromEffect(
                      log.error(s"${id} shard ${shardID} stream failed with " + e + ": " + e.getStackTrace)
                    ) *> ZStream.fail(e)
                }
          }
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
              .mapConcat(_.toList)
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
      producer   <- produceRecords(streamName, nrRecords).tapError(e => log.error(s"Producer error: ${e}")).fork
//      _          <- producer.join
      workers    <- ZIO.foreach(1 to nrNativeWorkers)(id => worker(s"worker${id}").runDrain.fork)
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
      _          <- ZIO.sleep(runtime) raceFirst ZIO.foreachPar_(kclWorkers ++ workers)(_.join)
      _           = println("Interrupting app")
      _          <- producer.interruptFork
      _          <- ZIO.foreachPar(kclWorkers)(_.interrupt)
      _          <- ZIO.foreachPar(workers)(_.interrupt)
    } yield ExitCode.success
  }.foldCauseM(e => log.error(s"Program failed: ${e.prettyPrint}", e).as(ExitCode.failure), ZIO.succeed(_))
    .provideCustomLayer(
      awsEnv // TODO switch back!!
      // localStackEnv
    )

  val loggingEnv = Slf4jLogger.make((_, logEntry) => logEntry, Some(getClass.getName))

  val localStackEnv =
    LocalStackServices.env >+>
      (AdminClient.live ++ Client.live ++ DynamoDbLeaseRepository.live).orDie ++
        loggingEnv

  val awsEnv: ZLayer[Clock, Nothing, AdminClient with Client with DynamicConsumer with Logging with Has[
    DynamoDbAsyncClient
  ] with LeaseRepository with Has[CloudWatchAsyncClient] with Has[CloudWatchMetricsPublisherConfig]] = {
    val httpClient    = HttpClient.make(
      maxConcurrency = 100,
      build = // _.proxyConfiguration(ProxyConfiguration.builder().host("localhost").port(9090).build())
        _.buildWithDefaults(
          AttributeMap.builder.put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, java.lang.Boolean.TRUE).build()
        )
    )
    val kinesisClient = kinesisAsyncClientLayer()
    val cloudWatch    = cloudWatchAsyncClientLayer()
    val dynamo        = dynamoDbAsyncClientLayer()
    val awsClients    = (httpClient >>> (kinesisClient ++ cloudWatch ++ dynamo)).orDie

    val client      = Client.live.orDie
    val adminClient = AdminClient.live.orDie

    val leaseRepo       = DynamoDbLeaseRepository.live
    val dynamicConsumer = DynamicConsumer.live
    val logging         = loggingEnv

    val metricsPublisherConfig = ZLayer.succeed(CloudWatchMetricsPublisherConfig())

    ZLayer.requires[Clock] >+>
      awsClients >+>
      (client ++ adminClient ++ dynamicConsumer ++ leaseRepo ++ logging ++ metricsPublisherConfig)
  }

  def produceRecords(streamName: String, nrRecords: Int) =
    Producer.make(streamName, Serde.asciiString).use { producer =>
      ZStream
        .range(1, nrRecords)
        .map(i => ProducerRecord(s"key$i", s"msg$i"))
        .chunkN(30)
        .mapChunksM(
          producer
            .produceChunk(_)
            .tap(_ => log.debug("Produced chunk"))
            .tapError(e => putStrLn(s"error: $e").provideLayer(Console.live))
            .retry(retryOnResourceNotFound && Schedule.recurs(1))
            .as(Chunk.unit)
            .tapCause(e => log.error("Producing records chunk failed, will retry", e))
            .retry(Schedule.exponential(1.second))
            .fork
            .map(Chunk.single(_))
            .delay(1.second) // TODO Until we fix throttling bug in Producer
        )
        .mapMPar(1)(_.join)
        .runDrain
        .tapCause(e => log.error("Producing records chunk failed", e))
    }
}
