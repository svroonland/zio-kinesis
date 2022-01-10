package nl.vroste.zio.kinesis.client.zionative

import nl.vroste.zio.kinesis.client.serde.Serde
import zio._

import zio.Console.printLine

/**
 * Basic usage example for `Consumer.consumeWith` convenience method
 */
object ConsumeWithExample extends zio.ZIOAppDefault {
  override def run: ZIO[zio.ZEnv with ZIOAppArgs, Any, Any] =
    Consumer
      .consumeWith(
        streamName = "my-stream",
        applicationName = "my-application",
        deserializer = Serde.asciiString,
        workerIdentifier = "worker1",
        checkpointBatchSize = 1000L,
        checkpointDuration = 5.minutes
      )(record => printLine(s"Processing record $record"))
      .provideCustomLayer(Consumer.defaultEnvironment)
      .exitCode
}
