package nl.vroste.zio.kinesis.client.zionative

import nl.vroste.zio.kinesis.client.serde.Serde
import zio._
import zio.console.putStrLn
import zio.duration._
import zio.logging.slf4j.Slf4jLogger

/**
 * Basic usage example for `Consumer.consumeWith` convenience method
 */
object ConsumeWithExample extends zio.App {
  private val loggingLayer = Slf4jLogger.make((_, logEntry) => logEntry, Some(getClass.getName))

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    Consumer
      .consumeWith(
        streamName = "my-stream",
        applicationName = "my-application",
        deserializer = Serde.asciiString,
        workerIdentifier = "worker1",
        checkpointBatchSize = 1000L,
        checkpointDuration = 5.minutes
      )(record => putStrLn(s"Processing record $record"))
      .provideCustomLayer(loggingLayer ++ Consumer.defaultEnvironment ++ loggingLayer)
      .exitCode
}
