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
import zio.logging.{ log, Logging }
import zio.logging.slf4j.Slf4jLogger
import zio.stream.{ ZStream, ZTransducer }
import zio.{ Chunk, ExitCode, Has, Schedule, ZIO, ZLayer }

/**
 * Example app that shows the ZIO-native and KCL workers running in parallel
 */
object ExampleApp extends zio.App {
  val streamName      = "zio-test-stream-4" // + java.util.UUID.randomUUID().toString
  val nrRecords       = 200000
  val nrShards        = 11
  val nrNativeWorkers = 1
  val nrKclWorkers    = 1
  val applicationName = "testApp-7"         // + java.util.UUID.randomUUID().toString(),

  override def run(
    args: List[String]
  ): ZIO[zio.ZEnv, Nothing, ExitCode] = {

    def worker(id: String) =
      ZStream.fromEffect(
        zio.random.nextIntBetween(0, 3000).flatMap(d => ZIO.sleep(d.millis))
      ) *> Consumer
        .shardedStream(
          streamName,
          applicationName = applicationName,
          deserializer = Serde.asciiString,
          fetchMode = FetchMode.Polling(),
          emitDiagnostic = {
            case _: DiagnosticEvent.PollComplete => ZIO.unit
            case ev                              => log.info(id + ": " + ev.toString).provideLayer(loggingEnv)
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
              .aggregateAsyncWithin(ZTransducer.last, Schedule.fixed(5.second))
              .mapConcat(_.toList)
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

    def kclWorker(id: String) =
      ZStream.fromEffect(
        zio.random.nextIntBetween(0, 3000).flatMap(d => ZIO.sleep(d.millis))
      ) *> DynamicConsumer
        .shardedStream(
          streamName,
          applicationName = applicationName,
          deserializer = Serde.asciiString,
          isEnhancedFanOut = false,
          workerIdentifier = id
        )
        .flatMapPar(Int.MaxValue) {
          case (shardID, shardStream, checkpointer) =>
            shardStream
              .tap(r =>
                checkpointer
                  .stageOnSuccess(putStrLn(s"${id} Processing record $r").when(false))(r)
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
      kclWorkers <- ZIO.foreach((1 + nrNativeWorkers) to (nrKclWorkers + nrNativeWorkers))(id =>
                      kclWorker(s"worker${id}").runDrain.fork
                    )
      _          <- ZIO.raceAll(ZIO.sleep(2.minute), (workers ++ kclWorkers).map(_.join))
      _           = println("Interrupting app")
      _          <- producer.interrupt
      _          <- ZIO.foreachPar_(workers)(_.interrupt)
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
    (kinesisAsyncClientLayer(Client.adjustKinesisClientBuilder(KinesisAsyncClient.builder())).orDie ++
      dynamoDbAsyncClientLayer().orDie ++
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
            .delay(1.second)
        )
        .mapMPar(1)(_.join)
        .runDrain
    }
}
