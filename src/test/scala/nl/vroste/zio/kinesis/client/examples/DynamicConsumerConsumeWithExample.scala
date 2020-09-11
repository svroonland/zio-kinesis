package nl.vroste.zio.kinesis.client.examples

import nl.vroste.zio.kinesis.client._
import nl.vroste.zio.kinesis.client.DynamicConsumer
import nl.vroste.zio.kinesis.client.serde.Serde
import zio._
import zio.clock.Clock
import zio.console.{ putStrLn, Console }
import zio.duration._
import zio.logging.Logging

/**
 * Basic usage example for `DynamicConsumer.consumeWith` convenience method
 */
object DynamicConsumerConsumeWithExample extends zio.App {
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
      )(record => putStrLn(s"Processing record $record"))
      .provideCustomLayer(loggingLayer ++ defaultAwsLayer >>> DynamicConsumer.live ++ loggingLayer)
      .exitCode
}
