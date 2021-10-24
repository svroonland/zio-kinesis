package nl.vroste.zio.kinesis.client.examples

import nl.vroste.zio.kinesis.client.HttpClientBuilder
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.zionative.Consumer
import nl.vroste.zio.kinesis.client.zionative.metrics.{ CloudWatchMetricsPublisher, CloudWatchMetricsPublisherConfig }
import zio._

import zio.logging.Logging
import zio.{ Console, Has }
import zio.Console.printLine

object NativeConsumerWithMetricsExample extends zio.ZIOAppDefault {

  val applicationName  = "my-application"
  val workerIdentifier = "worker1"

  val metricsConfig = CloudWatchMetricsPublisherConfig()

  override def run: ZIO[zio.ZEnv with Has[ZIOAppArgs], Any, Any] =
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
                .tap(record => printLine(s"Processing record ${record} on shard ${shardId}"))
                .tap(checkpointer.stage(_))
                .via(checkpointer.checkpointBatched[Has[Console]](nr = 1000, interval = 5.minutes))
          }
          .runDrain
      }
      .provideCustomLayer(
        (HttpClientBuilder.make() >>> Consumer.defaultEnvironment) ++ loggingLayer ++ ZLayer
          .succeed(metricsConfig)
      )
      .exitCode

  val loggingLayer = Logging.console() >>> Logging.withRootLoggerName(getClass.getName)
}
