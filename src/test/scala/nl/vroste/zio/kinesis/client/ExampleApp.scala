package nl.vroste.zio.kinesis.client
import java.util.UUID

import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.TestUtil.retryOnResourceNotFound
import nl.vroste.zio.kinesis.client.serde.Serde
import zio.console._
import zio.duration._
import zio.stream.{ ZStream, ZTransducer }
import zio.{ Chunk, ExitCode, Schedule, ZIO }
import software.amazon.kinesis.exceptions.ShutdownException

object ExampleApp extends zio.App {

  override def run(
    args: List[String]
  ): ZIO[zio.ZEnv, Nothing, ExitCode] = {

    val streamName = "zio-test-stream-" + UUID.randomUUID().toString

    for {
      _ <- TestUtil.createStreamUnmanaged(streamName, 10)
      _ <- produceRecords(streamName, 20000).fork
      _ <- LocalStackDynamicConsumer
             .shardedStream(
               streamName,
               applicationName = "testApp-" + UUID.randomUUID().toString(),
               deserializer = Serde.asciiString
             )
             .flatMapPar(Int.MaxValue) {
               case (shardID, shardStream, checkpointer) =>
                 shardStream
                   .tap(r => checkpointer.stageOnSuccess(putStrLn(s"Processing record $r"))(r)) // .delay(100.millis))
                   .aggregateAsyncWithin(ZTransducer.last, Schedule.fixed(1.second))
                   .mapConcat(_.toList)
                   .tap { _ =>
                     putStrLn(s"Checkpointing ${shardID}") *> checkpointer.checkpoint
                   }
                   .catchSome {
                     // This happens when the lease for the shard is lost. Best we can do is end the stream.
                     case _: ShutdownException => ZStream.empty
                   }
             }
             .runCollect
             .fork
      _ <- putStrLn("App started")
      _ <- ZIO.unit.delay(15.seconds)
      _ <- putStrLn("Exiting app")

    } yield ExitCode.success
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
