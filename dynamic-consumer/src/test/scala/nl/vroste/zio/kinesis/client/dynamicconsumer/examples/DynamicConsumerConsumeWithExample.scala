package nl.vroste.zio.kinesis.client.dynamicconsumer.examples

import nl.vroste.zio.kinesis.client.defaultAwsLayer
import nl.vroste.zio.kinesis.client.dynamicconsumer.DynamicConsumer
import nl.vroste.zio.kinesis.client.serde.Serde
import zio.Console.printLine
import zio._

/**
 * Basic usage example for `DynamicConsumer.consumeWith` convenience method
 */
object DynamicConsumerConsumeWithExample extends ZIOAppDefault {
  override def run: ZIO[Any with ZIOAppArgs with Scope, Any, Any] =
    DynamicConsumer
      .consumeWith(
        streamIdentifier = "my-stream",
        applicationName = "my-application",
        deserializer = Serde.asciiString,
        workerIdentifier = "worker1",
        checkpointBatchSize = 1000L,
        checkpointDuration = 5.minutes
      )(record => printLine(s"Processing record $record"))
      .provideLayer(defaultAwsLayer >+> DynamicConsumer.live)
      .exitCode
}
