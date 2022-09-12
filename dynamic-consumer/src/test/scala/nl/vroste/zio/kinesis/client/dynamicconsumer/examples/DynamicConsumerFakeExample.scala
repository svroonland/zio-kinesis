package nl.vroste.zio.kinesis.client.dynamicconsumer.examples

import nl.vroste.zio.kinesis.client.dynamicconsumer.DynamicConsumer
import nl.vroste.zio.kinesis.client.dynamicconsumer.DynamicConsumer.Record
import nl.vroste.zio.kinesis.client.dynamicconsumer.fake.DynamicConsumerFake
import nl.vroste.zio.kinesis.client.serde.Serde
import zio.Console.printLine
import zio.stream.ZStream
import zio._

/**
 * Basic usage example for `DynamicConsumerFake`
 */
object DynamicConsumerFakeExample extends ZIOAppDefault {
  private val shards: ZStream[Any, Nothing, (String, ZStream[Any, Throwable, Chunk[Byte]])] =
    DynamicConsumerFake.shardsFromStreams(Serde.asciiString, ZStream("msg1", "msg2"), ZStream("msg3", "msg4"))

  override def run: ZIO[Any with ZIOAppArgs with Scope, Any, Any] =
    for {
      refCheckpointedList <- Ref.make[Seq[Record[Any]]](Seq.empty)
      exitCode            <- DynamicConsumer
                               .consumeWith(
                                 streamName = "my-stream",
                                 applicationName = "my-application",
                                 deserializer = Serde.asciiString,
                                 workerIdentifier = "worker1",
                                 checkpointBatchSize = 1000L,
                                 checkpointDuration = 5.minutes
                               )(record => printLine(s"Processing record $record").orDie)
                               .provideLayer(DynamicConsumer.fake(shards, refCheckpointedList))
                               .exitCode
      _                   <- printLine(s"refCheckpointedList=$refCheckpointedList").orDie
    } yield exitCode

}
