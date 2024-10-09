package nl.vroste.zio.kinesis.client.zionative

import nl.vroste.zio.kinesis.client.serde.Serde
import zio.Console.printLine
import zio._

/**
 * Basic usage example for `Consumer.consumeWith` convenience method
 */
object ConsumeWithExample extends ZIOAppDefault {
  override def run: ZIO[Any with ZIOAppArgs with Scope, Any, Any] =
    Consumer
      .consumeWith(
        streamIdentifier = "my-stream",
        applicationName = "my-application",
        deserializer = Serde.asciiString,
        workerIdentifier = "worker1",
        consumptionBehaviour = ConsumptionBehaviour.default(
          checkpointBatchSize = 1000,
          checkpointDuration = 5.minutes
        )
      )(record => printLine(s"Processing record $record"))
      .provideLayer(Consumer.defaultEnvironment)
      .exitCode
}
