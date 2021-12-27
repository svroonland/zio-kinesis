package nl.vroste.zio.kinesis.client.examples

import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.zionative.Consumer
import zio._
import zio.console.{ putStrLn, Console }
import zio.duration._
import zio.logging.Logging

object NativeConsumerBasicUsageExample extends zio.App {
  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    Consumer
      .shardedStream(
        streamName = "my-stream",
        applicationName = "my-application",
        deserializer = Serde.asciiString,
        workerIdentifier = "worker1"
      )
      .flatMapPar(Int.MaxValue) { case (shardId, shardStream, checkpointer) =>
        shardStream
          .tap(record => putStrLn(s"Processing record ${record} on shard ${shardId}"))
          .tap(checkpointer.stage(_))
          .via(checkpointer.checkpointBatched[Console](nr = 1000, interval = 5.minutes))
      }
      .runDrain
      .provideCustomLayer(Consumer.defaultEnvironment ++ loggingLayer)
      .exitCode

  val loggingLayer = Logging.console() >>> Logging.withRootLoggerName(getClass.getName)
}
