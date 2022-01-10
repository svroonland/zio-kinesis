package nl.vroste.zio.kinesis.client.dynamicconsumer.examples

import nl.vroste.zio.kinesis.client.defaultAwsLayer
import nl.vroste.zio.kinesis.client.dynamicconsumer.DynamicConsumer
import nl.vroste.zio.kinesis.client.serde.Serde
import zio.duration.durationInt
import zio.{ ExitCode, URIO, ZLayer }
import zio.Console.printLine

/**
 * Basic usage example for `DynamicConsumer.consumeWith` convenience method
 */
object DynamicConsumerConsumeWithExample extends zio.ZIOAppDefault {
  val loggingLayer: ZLayer[Any, Nothing, Logging] =
    (Console.live ++ Clock.live) >>> Logging.console() >>> Logging.withRootLoggerName(getClass.getName)

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    DynamicConsumer
      .consumeWith(
        streamName = "my-stream",
        applicationName = "my-application",
        deserializer = Serde.asciiString,
        workerIdentifier = "worker1",
        checkpointBatchSize = 1000L,
        checkpointDuration = 5.minutes
      )(record => printLine(s"Processing record $record"))
      .provideCustomLayer((loggingLayer ++ defaultAwsLayer) >+> DynamicConsumer.live)
      .exitCode
}
