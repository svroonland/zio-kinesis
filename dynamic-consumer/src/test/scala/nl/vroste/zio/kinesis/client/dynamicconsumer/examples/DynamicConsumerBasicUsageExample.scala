package nl.vroste.zio.kinesis.client.dynamicconsumer.examples

import nl.vroste.zio.kinesis.client.defaultAwsLayer
import nl.vroste.zio.kinesis.client.dynamicconsumer.DynamicConsumer
import nl.vroste.zio.kinesis.client.serde.Serde
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.{ putStrLn, Console }
import zio.duration.durationInt
import zio.logging.Logging
import zio.{ ExitCode, URIO, ZLayer }

/**
 * Basic usage example for DynamicConsumer
 */
object DynamicConsumerBasicUsageExample extends zio.App {
  val loggingLayer: ZLayer[Any, Nothing, Logging] =
    (Console.live ++ Clock.live) >>> Logging.console() >>> Logging.withRootLoggerName(getClass.getName)

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    DynamicConsumer
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
          .via(checkpointer.checkpointBatched[Blocking with Console](nr = 1000, interval = 5.minutes))
      }
      .runDrain
      .provideCustomLayer((loggingLayer ++ defaultAwsLayer) >>> DynamicConsumer.live)
      .exitCode
}
