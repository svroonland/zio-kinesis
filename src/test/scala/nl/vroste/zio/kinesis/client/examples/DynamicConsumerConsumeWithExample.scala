package nl.vroste.zio.kinesis.client.examples

import nl.vroste.zio.kinesis.client.DynamicConsumer
import nl.vroste.zio.kinesis.client.serde.Serde
import zio._
import zio.console.putStrLn
import zio.duration._
import zio.logging.slf4j.Slf4jLogger

/**
 * Basic usage example for `DynamicConsumer.consumeWith` convenience method
 */
object DynamicConsumerConsumeWithExample extends zio.App {
  private val loggingEnv = Slf4jLogger.make((_, logEntry) => logEntry, Some(getClass.getName))

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
      .provideCustomLayer(DynamicConsumer.defaultEnvironment ++ loggingEnv)
      .exitCode
}
