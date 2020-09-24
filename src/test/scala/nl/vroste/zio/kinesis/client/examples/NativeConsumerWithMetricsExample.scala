package nl.vroste.zio.kinesis.client.examples

import io.github.vigoo.zioaws.core.config
import nl.vroste.zio.kinesis.client.HttpClientBuilder
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.zionative.Consumer
import nl.vroste.zio.kinesis.client.zionative.metrics.{ CloudWatchMetricsPublisher, CloudWatchMetricsPublisherConfig }
import zio._
import zio.console.{ putStrLn, Console }
import zio.duration._
import zio.logging.Logging

object NativeConsumerWithMetricsExample extends zio.App {

  val applicationName  = "my-application"
  val workerIdentifier = "worker1"

  val metricsConfig = CloudWatchMetricsPublisherConfig()

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    CloudWatchMetricsPublisher
      .make(applicationName, workerIdentifier)
      .use { metrics =>
        Consumer
          .shardedStream(
            streamName = "my-stream",
            applicationName = applicationName,
            deserializer = Serde.asciiString,
            workerIdentifier = workerIdentifier,
            emitDiagnostic = metrics.processEvent
          )
          .flatMapPar(Int.MaxValue) {
            case (shardId, shardStream, checkpointer) =>
              shardStream
                .tap(record => putStrLn(s"Processing record ${record} on shard ${shardId}"))
                .tap(checkpointer.stage(_))
                .via(checkpointer.checkpointBatched[Console](nr = 1000, interval = 5.second))
          }
          .runDrain
      }
      .provideCustomLayer(
        (HttpClientBuilder.make() >>> config.default >>> Consumer.defaultEnvironment) ++ loggingLayer ++ ZLayer
          .succeed(metricsConfig)
      )
      .exitCode

  val loggingLayer = Logging.console() >>> Logging.withRootLoggerName(getClass.getName)
}
