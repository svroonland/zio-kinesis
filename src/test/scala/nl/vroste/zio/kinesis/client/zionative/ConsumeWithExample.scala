package nl.vroste.zio.kinesis.client.zionative

import nl.vroste.zio.kinesis.client.serde.Serde
import zio._
import zio.clock.Clock
import zio.console.{ putStrLn, Console }
import zio.duration._
import zio.logging.Logging

/**
 * Basic usage example for `Consumer.consumeWith` convenience method
 */
object ConsumeWithExample extends zio.App {
  val loggingLayer: ZLayer[Any, Nothing, Logging] =
    (Console.live ++ Clock.live) >>> Logging.console() >>> Logging.withRootLoggerName(getClass.getName)

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
<<<<<<< HEAD
      .provideCustomLayer(Consumer.defaultEnvironment ++ loggingLayer)
=======
      .provideCustomLayer(loggingLayer ++ Consumer.defaultEnvironment ++ loggingLayer)
>>>>>>> origin/master
      .exitCode
}
