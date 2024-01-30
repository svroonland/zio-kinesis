package nl.vroste.zio.kinesis.client.examples

import nl.vroste.zio.kinesis.client.HttpClientBuilder
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.zionative.Consumer
import nl.vroste.zio.kinesis.client.zionative.metrics.{ CloudWatchMetricsPublisher, CloudWatchMetricsPublisherConfig }
import zio.Console.printLine
import zio._

object NativeConsumerWithMetricsExample extends ZIOAppDefault {

  val applicationName  = "my-application"
  val workerIdentifier = "worker1"

  val metricsConfig = CloudWatchMetricsPublisherConfig()

  override def run: ZIO[ZIOAppArgs with Scope, Any, Any] =
    CloudWatchMetricsPublisher
      .make(applicationName, workerIdentifier)
      .flatMap { metrics =>
        Consumer
          .shardedStream(
            streamIdentifier = "my-stream",
            applicationName = applicationName,
            deserializer = Serde.asciiString,
            workerIdentifier = workerIdentifier,
            emitDiagnostic = metrics.processEvent
          )
          .flatMapPar(Int.MaxValue) { case (shardId, shardStream, checkpointer) =>
            shardStream
              .tap(record => printLine(s"Processing record ${record} on shard ${shardId}"))
              .tap(checkpointer.stage(_))
              .viaFunction(checkpointer.checkpointBatched[Any](nr = 1000, interval = 5.minutes))
          }
          .runDrain
      }
      .provideLayer(
        (HttpClientBuilder.make() >>> Consumer.defaultEnvironment) ++ ZLayer.succeed(metricsConfig) ++ Scope.default
      )
      .exitCode

}
