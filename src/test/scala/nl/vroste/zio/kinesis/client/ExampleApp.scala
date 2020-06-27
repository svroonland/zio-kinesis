package nl.vroste.zio.kinesis.client
import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.TestUtil.retryOnResourceNotFound
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.zionative.{ Consumer, DiagnosticEvent, FetchMode, ShardLeaseLost }
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.kinesis.exceptions.ShutdownException
import zio.console._
import zio.duration._
import zio.logging.slf4j.Slf4jLogger
import zio.logging.{ log, Logging }
import zio.stream.{ ZStream, ZTransducer }
import zio._

/**
 * Example app that shows the ZIO-native and KCL workers running in parallel
 */
object ExampleApp extends zio.App {
  val streamName      = "zio-test-stream-7" // + java.util.UUID.randomUUID().toString
  val nrRecords       = 2000000
  val nrShards        = 200
  val nrNativeWorkers = 1
  val nrKclWorkers    = 0
  val applicationName = "testApp-12"        // + java.util.UUID.randomUUID().toString(),
  val runtime         = 3.minute

  override def run(
    args: List[String]
  ): ZIO[zio.ZEnv, Nothing, ExitCode] = {

    def worker(id: String) =
      ZStream.fromEffect(
        zio.random.nextIntBetween(0, 20000).flatMap(d => ZIO.sleep(d.millis))
      ) *> Consumer
        .shardedStream(
          streamName,
          applicationName = applicationName,
          deserializer = Serde.asciiString,
          fetchMode = FetchMode.EnhancedFanOut(maxSubscriptionsPerSecond = 50), // FetchMode.Polling(),
          emitDiagnostic = {
            case ev: DiagnosticEvent.PollComplete =>
              log
                .info(
                  id + s": PollComplete for ${ev.nrRecords} records of ${ev.shardId}, behind latest: ${ev.duration.toMillis} millis"
                )
                .provideLayer(loggingEnv)
            case ev                               => log.info(id + ": " + ev.toString).provideLayer(loggingEnv)
          },
          workerId = id
        )
        .flatMapPar(Int.MaxValue) {
          case (shardID, shardStream, checkpointer) =>
            shardStream
              .tap(r =>
                checkpointer
                  .stageOnSuccess(putStrLn(s"${id} Processing record $r").when(false))(r)
              )
              .aggregateAsyncWithin(ZTransducer.collectAllN(10000), Schedule.fixed(5.second))
              .tap(rs => log.info(s"${id} processed ${rs.size} records on shard ${shardID}"))
              .mapConcat(_.lastOption.toList)
              .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
              .tap(_ => checkpointer.checkpoint)
              .catchAll {
                case Right(ShardLeaseLost) =>
                  ZStream.empty
                case Left(e)               =>
                  ZStream.fromEffect(
                    log.error(s"${id} shard ${shardID} stream failed with" + e + ": " + e.getStackTrace)
                  ) *> ZStream.fail(e)
              }
        }

    def kclWorker(id: String, requestShutdown: Promise[Nothing, Unit]) =
      ZStream.fromEffect(
        zio.random.nextIntBetween(0, 1000).flatMap(d => ZIO.sleep(d.millis))
      ) *> DynamicConsumer
        .shardedStream(
          streamName,
          applicationName = applicationName,
          deserializer = Serde.asciiString,
          isEnhancedFanOut = true,
          workerIdentifier = id,
          requestShutdown = requestShutdown.await
        )
        .flatMapPar(Int.MaxValue) {
          case (shardID, shardStream, checkpointer) =>
            shardStream
              .tap(r =>
                checkpointer
                  .stageOnSuccess(putStrLn(s"${id} Processing record $r").when(true))(r)
              )
              .aggregateAsyncWithin(ZTransducer.last, Schedule.fixed(5.second))
              .mapConcat(_.toList)
              .tap(_ => checkpointer.checkpoint)
              .catchAll {
                case _: ShutdownException => // This will be thrown when the shard lease has been stolen
                  // Abort the stream when we no longer have the lease
                  ZStream.empty
                case e                    =>
                  ZStream.fromEffect(
                    log.error(s"${id} shard ${shardID} stream failed with" + e + ": " + e.getStackTrace)
                  ) *> ZStream.fail(e)
              }
        }

    for {
      _          <- TestUtil.createStreamUnmanaged(streamName, nrShards)
      producer   <- produceRecords(streamName, nrRecords).fork
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
      _          <- ZIO.sleep(runtime)
      _           = println("Interrupting app")
      _          <- producer.interruptFork
      _          <- ZIO.foreachPar(kclWorkers)(_.interrupt)
      _          <- ZIO.foreachPar(workers)(_.interrupt)
    } yield ExitCode.success
  }.orDie.provideCustomLayer(
    awsEnv // TODO switch back!!
    // localStackEnv
  )

  val loggingEnv = Slf4jLogger.make((_, logEntry) => logEntry, Some(getClass.getName))

  val localStackEnv =
    (LocalStackServices.kinesisAsyncClientLayer >>> (AdminClient.live ++ Client.live)).orDie ++ LocalStackServices.dynamoDbClientLayer.orDie ++ loggingEnv

  val awsEnv
    : ZLayer[Any, Nothing, AdminClient with Client with DynamicConsumer with Logging with Has[DynamoDbAsyncClient]] =
    (kinesisAsyncClientLayer(
      Client.adjustKinesisClientBuilder(KinesisAsyncClient.builder(), maxConcurrency = 100)
    ).fresh.orDie ++
      dynamoDbAsyncClientLayer().fresh.orDie ++
      cloudWatchAsyncClientLayer().orDie) >>>
      (AdminClient.live.orDie ++ Client.live.orDie ++ DynamicConsumer.live ++ loggingEnv ++ ZLayer
        .requires[Has[DynamoDbAsyncClient]])

  def produceRecords(streamName: String, nrRecords: Int) =
    Producer.make(streamName, Serde.asciiString).use { producer =>
      ZStream
        .range(1, nrRecords)
        .map(i => ProducerRecord(s"key$i", s"msg$i"))
        .chunkN(499)
        .mapChunksM(
          producer
            .produceChunk(_)
            .tapError(e => putStrLn(s"error: $e").provideLayer(Console.live))
            .retry(retryOnResourceNotFound && Schedule.recurs(1))
            .as(Chunk.unit)
            .fork
            .map(Chunk.single(_))
//            .delay(1.second)
        )
        .mapMPar(10)(_.join)
        .runDrain
    }
}
