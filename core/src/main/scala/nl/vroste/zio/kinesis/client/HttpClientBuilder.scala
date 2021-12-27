package nl.vroste.zio.kinesis.client
import io.github.vigoo.zioaws.core.httpclient
import io.github.vigoo.zioaws.core.httpclient.HttpClient
import software.amazon.awssdk.http.Protocol
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.http.nio.netty.{ Http2Configuration, NettyNioAsyncHttpClient }
import zio.duration._
import zio.{ Task, ZIO, ZLayer }

object HttpClientBuilder {

  /**
   * Builder for SdkAsyncHttpClient
   *
   * Some settings' defaults are recommended settings for Kinesis streaming using HTTP 2
   *
   * @param maxConcurrency
   *   Maximum concurrent connections. Recommended to set higher than the amount of shards you expect to process
   * @param initialWindowSize
   * @param healthCheckPingPeriod
   * @param maxPendingConnectionAcquires
   * @param connectionAcquisitionTimeout
   * @param readTimeout
   * @param allowHttp2
   *   Allow services that support HTTP2 to use it. Set to false to only use HTTP 1
   */
  def make(
    maxConcurrency: Int = Int.MaxValue,
    initialWindowSize: Int = 512 * 1024, // 512 KB, see https://github.com/awslabs/amazon-kinesis-client/pull/706
    healthCheckPingPeriod: Duration = 10.seconds,
    maxPendingConnectionAcquires: Int = 10000,
    connectionAcquisitionTimeout: Duration = 30.seconds,
    writeTimeout: Duration = 30.seconds,
    readTimeout: Duration = 30.seconds,
    allowHttp2: Boolean = true,
    build: NettyNioAsyncHttpClient.Builder => SdkAsyncHttpClient = _.build()
  ): ZLayer[Any, Nothing, HttpClient] =
    ZLayer.succeed {
      new HttpClient.Service {
        override def clientFor(serviceCaps: httpclient.ServiceHttpCapabilities): Task[SdkAsyncHttpClient] = {
          val protocol = if (allowHttp2 && serviceCaps.supportsHttp2) Protocol.HTTP2 else Protocol.HTTP1_1

          val builder = NettyNioAsyncHttpClient
            .builder()
            .maxConcurrency(maxConcurrency)
            .connectionAcquisitionTimeout(connectionAcquisitionTimeout.asJava)
            .maxPendingConnectionAcquires(maxPendingConnectionAcquires)
            .writeTimeout(writeTimeout)
            .readTimeout(readTimeout)
            .http2Configuration(
              Http2Configuration
                .builder()
                .initialWindowSize(initialWindowSize)
                .maxStreams(maxConcurrency.toLong)
                .healthCheckPingPeriod(healthCheckPingPeriod.asJava)
                .build()
            )
            .protocol(protocol)

          ZIO(build(builder))
        }
      }
    }
}
