package nl.vroste.zio.kinesis.client.dynamicconsumer.examples

import nl.vroste.zio.kinesis.client.defaultAwsLayer
import nl.vroste.zio.kinesis.client.dynamicconsumer.DynamicConsumer
import nl.vroste.zio.kinesis.client.serde.Serde
import zio.Console.printLine
import zio.{ durationInt, ZEnv, ZIO, ZIOAppArgs }

/**
 * Basic usage example for `DynamicConsumer.consumeWith` convenience method
 */
object DynamicConsumerConsumeWithExample extends zio.ZIOAppDefault {
  override def run: ZIO[ZEnv with ZIOAppArgs, Any, Any] =
    DynamicConsumer
      .consumeWith(
        streamName = "my-stream",
        applicationName = "my-application",
        deserializer = Serde.asciiString,
        workerIdentifier = "worker1",
        checkpointBatchSize = 1000L,
        checkpointDuration = 5.minutes
      )(record => printLine(s"Processing record $record"))
      .provideCustomLayer(defaultAwsLayer >+> DynamicConsumer.live)
      .exitCode
}
