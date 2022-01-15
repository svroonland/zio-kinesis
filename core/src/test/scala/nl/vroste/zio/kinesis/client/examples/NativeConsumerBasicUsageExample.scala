package nl.vroste.zio.kinesis.client.examples

import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.zionative.Consumer
import zio.Console.printLine
import zio.{ Console, _ }

object NativeConsumerBasicUsageExample extends zio.ZIOAppDefault {
  override def run: ZIO[zio.ZEnv with ZIOAppArgs, Any, Any] =
    Consumer
      .shardedStream(
        streamName = "my-stream",
        applicationName = "my-application",
        deserializer = Serde.asciiString,
        workerIdentifier = "worker1"
      )
      .flatMapPar(Int.MaxValue) { case (shardId, shardStream, checkpointer) =>
        shardStream
          .tap(record => printLine(s"Processing record ${record} on shard ${shardId}"))
          .tap(checkpointer.stage(_))
          .viaFunction(checkpointer.checkpointBatched[Console](nr = 1000, interval = 5.minutes))
      }
      .runDrain
      .provideCustomLayer(Consumer.defaultEnvironment)
      .exitCode
}
