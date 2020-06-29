package nl.vroste.zio.kinesis.client.zionative

import zio.UIO
import zio.ZLayer
import zio.ZIO
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import nl.vroste.zio.kinesis.client.zionative.metrics.CloudWatchMetricsPublisher
import zio.clock.Clock
import zio.logging.Logging
import zio.Has
import zio.Queue
import zio.ZManaged

object MetricsPublisher {
  trait Service {
    def processEvent(e: DiagnosticEvent): UIO[Unit]
  }

  def make(
    applicationName: String
  ): ZLayer[Clock with Logging with Has[CloudWatchAsyncClient], Nothing, Has[MetricsPublisher.Service]] =
    ZLayer.fromManaged {
      for {
        client <- ZIO.service[CloudWatchAsyncClient].toManaged_
        q      <- Queue.bounded[DiagnosticEvent](1000).toManaged_
        c       = new CloudWatchMetricsPublisher(client, q, applicationName)
        _      <- c.processQueue.forkManaged
        _      <- ZManaged.finalizer(q.shutdown)
      } yield c
    }
}
