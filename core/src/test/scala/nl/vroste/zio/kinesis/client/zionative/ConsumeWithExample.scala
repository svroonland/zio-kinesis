package nl.vroste.zio.kinesis.client.zionative

import nl.vroste.zio.kinesis.client.serde.Serde
import zio.Console.printLine
import zio._

/**
 * Basic usage example for `Consumer.consumeWith` convenience method
 */
object ConsumeWithExample extends zio.ZIOAppDefault {
  override def run: ZIO[Any with ZIOAppArgs with Scope, Any, Any] =
    Consumer
      .consumeWith(
        streamName = "my-stream",
        applicationName = "my-application",
        deserializer = Serde.asciiString,
        workerIdentifier = "worker1",
        checkpointBatchSize = 1000L,
        checkpointDuration = 5.minutes
      )(record => printLine(s"Processing record $record"))
      .provideLayer(Consumer.defaultEnvironment)
      .exitCode
}
