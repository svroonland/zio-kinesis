package nl.vroste.zio.kinesis.client.examples

import nl.vroste.zio.kinesis.client.DynamicConsumer
import nl.vroste.zio.kinesis.client.serde.Serde
import zio._
import zio.blocking.Blocking
import zio.console.{ putStrLn, Console }
import zio.duration._
import zio.logging.slf4j.Slf4jLogger

/**
 * Basic usage example for DynamicConsumer
 */
object DynamicConsumerBasicUsageExample extends zio.App {
  private val loggingEnv = Slf4jLogger.make((_, logEntry) => logEntry, Some(getClass.getName))

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    DynamicConsumer
      .shardedStream(
        streamName = "my-stream",
        applicationName = "my-application",
        deserializer = Serde.asciiString,
        workerIdentifier = "worker1"
      )
      .flatMapPar(Int.MaxValue) {
        case (shardId, shardStream, checkpointer) =>
          shardStream
            .tap(record => putStrLn(s"Processing record ${record} on shard ${shardId}"))
            .tap(checkpointer.stage(_))
            .via(checkpointer.checkpointBatched[Blocking with Console](nr = 1000, interval = 5.second))
      }
      .runDrain
      .provideCustomLayer(loggingEnv ++ DynamicConsumer.defaultAwsEnvironment >>> DynamicConsumer.live)
      .exitCode
}
