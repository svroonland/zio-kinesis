package nl.vroste.zio.kinesis.client
import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.TestUtil.retryOnResourceNotFound
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.zionative.{ Consumer, FetchMode, ShardLeaseLost }
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import zio.console._
import zio.duration._
import zio.logging.log
import zio.logging.slf4j.Slf4jLogger
import zio.stream.{ ZStream, ZTransducer }
import zio.{ Chunk, ExitCode, Schedule, ZIO }

object ExampleApp extends zio.App {

  override def run(
    args: List[String]
  ): ZIO[zio.ZEnv, Nothing, ExitCode] = {

    val streamName      = "zio-test-stream-4" // + java.util.UUID.randomUUID().toString
    val nrRecords       = 200000
    val nrShards        = 11
    val nrWorkers       = 3
    val applicationName = "testApp-6"         // + java.util.UUID.randomUUID().toString(),

    def worker(id: String) =
      ZStream.fromEffect(
        zio.random.nextIntBetween(0, 3000).flatMap(d => ZIO.sleep(d.millis))
      ) *> Consumer
        .shardedStream(
          streamName,
          applicationName = applicationName,
          deserializer = Serde.asciiString,
          fetchMode = FetchMode.EnhancedFanOut,
          emitDiagnostic = ev => log.info(id + ": " + ev.toString).provideLayer(loggingEnv),
          workerId = id
        )
        .flatMapPar(Int.MaxValue) {
          case (shardID, shardStream, checkpointer) =>
            shardStream
              .tap(r =>
                checkpointer
                  .stageOnSuccess(putStrLn(s"Processing record $r").when(false))(r)
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

    for {
      _        <- TestUtil.createStreamUnmanaged(streamName, nrShards)
      producer <- produceRecords(streamName, nrRecords).fork
      _        <- producer.join
      workers  <- ZIO.foreach(1 to nrWorkers)(id => worker(s"worker${id}").runDrain.fork)
      _        <- ZIO.raceAll(ZIO.sleep(2.minute), workers.map(_.join))
      _         = println("Interrupting app")
      _        <- producer.interrupt
      _        <- ZIO.foreachPar_(workers)(_.interrupt)
    } yield ExitCode.success
  }.orDie.provideCustomLayer(
    awsEnv // TODO switch back!!
    // localStackEnv
  )

  val loggingEnv = Slf4jLogger.make((_, logEntry) => logEntry, Some(getClass.getName))

  val localStackEnv =
    (LocalStackServices.kinesisAsyncClientLayer >>> (AdminClient.live ++ Client.live)).orDie ++ LocalStackServices.dynamoDbClientLayer.orDie ++ loggingEnv

  val awsEnv =
    (kinesisAsyncClientLayer(
      Client.adjustKinesisClientBuilder(KinesisAsyncClient.builder())
    ) >>> (AdminClient.live ++ Client.live)).orDie ++ dynamoDbAsyncClientLayer().orDie ++ loggingEnv

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
          // .delay(1.second)
        )
        .mapMPar(1000)(_.join)
        .runDrain
    }
}
