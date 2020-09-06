package nl.vroste.zio.kinesis.client
import akka.actor.ActorSystem
import akka.http.scaladsl.settings.ConnectionPoolSettings
import com.github.matsluni.akkahttpspi.AkkaHttpClient
import io.github.vigoo.zioaws.cloudwatch.CloudWatch
import io.github.vigoo.zioaws.core.config
import io.github.vigoo.zioaws.core.httpclient.HttpClient
import io.github.vigoo.zioaws.dynamodb.DynamoDb
import io.github.vigoo.zioaws.kinesis.Kinesis
import io.github.vigoo.zioaws.kinesis.model.{ ScalingType, UpdateShardCountRequest }
import io.github.vigoo.zioaws.{ cloudwatch, dynamodb, kinesis }
import nl.vroste.zio.kinesis.client.TestUtil.retryOnResourceNotFound
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.zionative.Consumer.InitialPosition
import nl.vroste.zio.kinesis.client.zionative._
import nl.vroste.zio.kinesis.client.zionative.leaserepository.DynamoDbLeaseRepository
import nl.vroste.zio.kinesis.client.zionative.metrics.{ CloudWatchMetricsPublisher, CloudWatchMetricsPublisherConfig }
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
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
  val streamName                      = "zio-test-stream-25" // + java.util.UUID.randomUUID().toString
  val nrRecords                       = 2000000
  val produceRate                     = 100                  // Nr records to produce per second
  val nrShards                        = 10
  val reshardFactor                   = 0.5
  val reshardAfter: Option[Duration]  = None                 // Some(10.seconds)
  val enhancedFanout                  = false
  val nrNativeWorkers                 = 2
  val nrKclWorkers                    = 0
  val applicationName                 = "testApp-14"         // + java.util.UUID.randomUUID().toString(),
  val runtime                         = 2.minute
  val maxRandomWorkerStartDelayMillis = 1 + 0 * 60 * 1000
  val recordProcessingTime: Duration  = 1.millisecond

  override def run(
    args: List[String]
  ): ZIO[zio.ZEnv, Nothing, ExitCode] = {

    def worker(id: String) =
      ZStream.unwrapManaged {
        for {
          metrics <- CloudWatchMetricsPublisher.make(applicationName, id)
//          delay   <- zio.random.nextIntBetween(0, maxRandomWorkerStartDelayMillis).map(_.millis).toManaged_
          delay    = 1.seconds
          _       <- log.info(s"Waiting ${delay.toMillis} ms to start worker ${id}").toManaged_
        } yield ZStream.fromEffect(ZIO.sleep(delay)) *> Consumer
          .shardedStream(
            streamName,
            applicationName = applicationName,
            deserializer = Serde.asciiString,
            workerIdentifier = id,
            fetchMode = if (enhancedFanout) FetchMode.EnhancedFanOut() else FetchMode.Polling(batchSize = 1000),
            initialPosition = InitialPosition.Latest,
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
                .tap(rs => log.info(s"${id} processed ${rs.size} records on shard ${shardID}").when(false))
                .mapConcat(_.lastOption.toList)
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
      _          <- reshardAfter
             .map(delay =>
               (log.info("Resharding") *>
                 kinesis.updateShardCount(
                   UpdateShardCountRequest(
                     streamName,
                     Math.ceil(nrShards * reshardFactor).toInt,
                     ScalingType.UNIFORM_SCALING
                   )
                 ))
                 .delay(delay)
             )
             .getOrElse(ZIO.unit)
             .fork
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

  val loggingLayer = Slf4jLogger.make((_, logEntry) => logEntry, Some(getClass.getName))

  val localStackEnv: ZLayer[
    Clock,
    Nothing,
    Kinesis with DynamoDb with CloudWatch with DynamicConsumer with Logging with LeaseRepository with Has[
      CloudWatchMetricsPublisherConfig
    ]
  ] =
    (LocalStackServices.env.orDie ++ loggingLayer) >+>
      (DynamicConsumer.live ++
        DynamoDbLeaseRepository.live ++
        ZLayer.succeed(CloudWatchMetricsPublisherConfig()))

  val awsEnv: ZLayer[
    Clock,
    Nothing,
    Kinesis with DynamoDb with CloudWatch with DynamicConsumer with Logging with LeaseRepository with Has[
      CloudWatchMetricsPublisherConfig
    ]
  ] = {
    // NETTY
//    val awsHttpClient = HttpClientBuilder.make(maxConcurrency = 100, allowHttp2 = false)

    // HTTP4S
//    val awsHttpClient = io.github.vigoo.zioaws.http4s.client()

    // AKKA HTTP
    val actorSystem =
      ZLayer.fromAcquireRelease(ZIO.effect(ActorSystem("test")))(sys => ZIO.fromFuture(_ => sys.terminate()).orDie)

    val awsHttpClient: ZLayer[Any, Throwable, Has[HttpClient.Service]] = actorSystem >>>
      ZLayer.fromServiceM { (actorSystem: ActorSystem) =>
        ZIO.runtime[Any].flatMap { runtime =>
          val ec = runtime.platform.executor.asEC

          ZIO(
            AkkaHttpClient
              .builder()
              .withConnectionPoolSettings(
                ConnectionPoolSettings(actorSystem).withMaxOpenRequests(100).withMaxConnections(1000)
              )
              .withActorSystem(actorSystem)
              .withExecutionContext(ec)
              .build()
          ).map { akkaClient =>
            new HttpClient.Service {
              override val client: SdkAsyncHttpClient = akkaClient
            }
          }
        }
      }

    val awsClients =
      (awsHttpClient >>> config.default >>> (kinesis.live ++ cloudwatch.live ++ dynamodb.live)).orDie

    val leaseRepo       = DynamoDbLeaseRepository.live
    val dynamicConsumer = DynamicConsumer.live
    val logging         = loggingLayer

    val metricsPublisherConfig = ZLayer.succeed(CloudWatchMetricsPublisherConfig())

    ZLayer.requires[Clock] >+>
      logging >+>
      awsClients >+>
      (dynamicConsumer ++ leaseRepo ++ metricsPublisherConfig)
  }

  def produceRecords(streamName: String, nrRecords: Int) =
    Producer.make(streamName, Serde.asciiString).use { producer =>
      ZStream
        .range(1, nrRecords)
        .map(i => ProducerRecord(s"key$i", s"msg$i"))
        .chunkN(produceRate)
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
