package nl.vroste.zio.kinesis.client.examples

import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.zionative.Consumer
import zio._

import zio.logging.Logging
import zio.{ Console, Has }
import zio.Console.printLine

object NativeConsumerBasicUsageExample extends zio.ZIOAppDefault {
  override def run: ZIO[zio.ZEnv with Has[ZIOAppArgs], Any, Any] =
    Consumer
      .shardedStream(
        streamName = "my-stream",
        applicationName = "my-application",
        deserializer = Serde.asciiString,
        workerIdentifier = "worker1"
      )
      .flatMapPar(Int.MaxValue) {
        case (shardId, shardStream, checkpointer) =>
          shardStream
            .tap(record => printLine(s"Processing record ${record} on shard ${shardId}"))
            .tap(checkpointer.stage(_))
            .via(checkpointer.checkpointBatched[Has[Console]](nr = 1000, interval = 5.minutes))
      }
      .runDrain
      .provideCustomLayer(Consumer.defaultEnvironment ++ loggingLayer)
      .exitCode

  val loggingLayer = Logging.console() >>> Logging.withRootLoggerName(getClass.getName)
}
