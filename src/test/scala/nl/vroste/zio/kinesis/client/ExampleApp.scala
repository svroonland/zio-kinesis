package nl.vroste.zio.kinesis.client
import java.util.UUID

import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.TestUtil.retryOnResourceNotFound
import nl.vroste.zio.kinesis.client.serde.Serde
import zio.console._
import zio.duration._
import zio.stream.{ ZStream, ZTransducer }
import zio.{ Chunk, ExitCode, Schedule, ZIO }

object ExampleApp extends zio.App {

  override def run(
    args: List[String]
  ): ZIO[zio.ZEnv, Nothing, ExitCode] = {

    val streamName = "zio-test-stream-" + UUID.randomUUID().toString

    TestUtil
      .createStreamUnmanaged(streamName, 10) *>
      (for {
        _ <- produceRecords(streamName, 20000).fork.toManaged_
        _ <- withGracefulShutdownOnInterrupt { interrupt =>
               LocalStackDynamicConsumer
                 .shardedStream(
                   streamName,
                   applicationName = "testApp",
                   deserializer = Serde.asciiString,
                   requestShutdown = interrupt
                 )
                 .flatMapPar(Int.MaxValue) {
                   case (shardID, shardStream) =>
                     shardStream
                       .tap(r => putStrLn(s"Got record $r")) // .delay(100.millis))
                       .aggregateAsyncWithin(ZTransducer.last, Schedule.fixed(1.second))
                       .mapConcat(_.toList)
                       .tap { r =>
                         putStrLn(s"Checkpointing ${shardID}") *> r.checkpoint
                       }
                 }
                 .runCollect
             }
      } yield ()).use_ {
        for {
          _ <- putStrLn("App started")
          _ <- ZIO.unit.delay(15.seconds)
          _ <- putStrLn("Exiting app")

        } yield ExitCode.success
      }
  }.orDie

  def produceRecords(streamName: String, nrRecords: Int) =
    (for {
      client   <- Client.build(LocalStackDynamicConsumer.kinesisAsyncClientBuilder)
      producer <- Producer.make(streamName, client, Serde.asciiString)
    } yield producer).use { producer =>
      val records =
        (1 to nrRecords).map(i => ProducerRecord(s"key$i", s"msg$i"))
      ZStream
        .fromIterable(records)
        .chunkN(10)
        .mapChunksM(
          producer
            .produceChunk(_)
            .tapError(e => putStrLn(s"error: $e").provideLayer(Console.live))
            .retry(retryOnResourceNotFound)
            .as(Chunk.unit)
            .delay(1.second)
        )
        .runDrain
    }
}
