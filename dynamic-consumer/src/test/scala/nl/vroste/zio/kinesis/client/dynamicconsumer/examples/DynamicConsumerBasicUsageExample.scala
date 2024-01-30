package nl.vroste.zio.kinesis.client.dynamicconsumer.examples

import nl.vroste.zio.kinesis.client.defaultAwsLayer
import nl.vroste.zio.kinesis.client.dynamicconsumer.DynamicConsumer
import nl.vroste.zio.kinesis.client.serde.Serde
import zio.Console.printLine
import zio._

/**
 * Basic usage example for DynamicConsumer
 */
object DynamicConsumerBasicUsageExample extends ZIOAppDefault {
  override def run: ZIO[Any with ZIOAppArgs with Scope, Any, Any] =
    DynamicConsumer
      .shardedStream(
        streamIdentifier = "my-stream",
        applicationName = "my-application",
        deserializer = Serde.asciiString,
        workerIdentifier = "worker1"
      )
      .flatMapPar(Int.MaxValue) { case (shardId, shardStream, checkpointer) =>
        shardStream
          .tap(record => printLine(s"Processing record ${record} on shard ${shardId}"))
          .tap(checkpointer.stage(_))
          .viaFunction(checkpointer.checkpointBatched[Any](nr = 1000, interval = 5.minutes))
      }
      .runDrain
      .provideLayer(defaultAwsLayer >>> DynamicConsumer.live)
      .exitCode
}
