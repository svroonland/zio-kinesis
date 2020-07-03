package nl.vroste.zio.kinesis

import software.amazon.awssdk.services.cloudwatch.{ CloudWatchAsyncClient, CloudWatchAsyncClientBuilder }
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient, DynamoDbAsyncClientBuilder }
import software.amazon.awssdk.services.kinesis.{ KinesisAsyncClient, KinesisAsyncClientBuilder }
import zio.{ Has, ZIO, ZLayer, ZManaged }
import zio.duration._
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.http.nio.netty.Http2Configuration
import software.amazon.awssdk.http.Protocol
import software.amazon.awssdk.http.async.SdkAsyncHttpClient

package object client {

  type AdminClient     = Has[AdminClient.Service]
  type Client          = Has[Client.Service]
  type DynamicConsumer = Has[DynamicConsumer.Service]

  def kinesisAsyncClientLayer(
    builder: KinesisAsyncClientBuilder = KinesisAsyncClient.builder
  ): ZLayer[Has[SdkAsyncHttpClient], Throwable, Has[KinesisAsyncClient]] =
    ZLayer.fromServiceManaged { (httpClient: SdkAsyncHttpClient) =>
      ZManaged.fromAutoCloseable(ZIO.effect(builder.httpClient(httpClient).build))
    }

  def cloudWatchAsyncClientLayer(
    builder: CloudWatchAsyncClientBuilder = CloudWatchAsyncClient.builder
  ): ZLayer[Has[SdkAsyncHttpClient], Throwable, Has[CloudWatchAsyncClient]] =
    ZLayer.fromServiceManaged { (httpClient: SdkAsyncHttpClient) =>
      ZManaged.fromAutoCloseable(ZIO.effect(builder.httpClient(httpClient).build))
    }

  def dynamoDbAsyncClientLayer(
    builder: DynamoDbAsyncClientBuilder = DynamoDbAsyncClient.builder
  ): ZLayer[Has[SdkAsyncHttpClient], Throwable, Has[DynamoDbAsyncClient]] =
    ZLayer.fromServiceManaged { (httpClient: SdkAsyncHttpClient) =>
      ZManaged.fromAutoCloseable(ZIO.effect(builder.httpClient(httpClient).build))
    }

  /**
   * Creates an optimized HTTP client for parallel shard streaming
   *
   * @param builder
   * @param maxConcurrency Set this to something like the number of leases + a bit more
   * @param initialWindowSize
   * @param healthCheckPingPeriod
   */
  def httpClientLayer(
    maxConcurrency: Int = Int.MaxValue,
    initialWindowSize: Int = 512 * 1024, // 512 KB, see https://github.com/awslabs/amazon-kinesis-client/pull/706
    healthCheckPingPeriod: Duration = 10.seconds,
    maxPendingConnectionAcquires: Int = 10000,
    connectionAcquisitionTimeout: Duration = 30.seconds,
    readTimeout: Duration = 30.seconds
  ): ZLayer[Any, Throwable, Has[SdkAsyncHttpClient]] =
    ZLayer.fromEffect {
      ZIO.effect {
        NettyNioAsyncHttpClient
          .builder()
          .maxConcurrency(maxConcurrency)
          .connectionAcquisitionTimeout(connectionAcquisitionTimeout.asJava)
          .maxPendingConnectionAcquires(maxPendingConnectionAcquires)
          .readTimeout(readTimeout.asJava)
          .http2Configuration(
            Http2Configuration
              .builder()
              .initialWindowSize(initialWindowSize)
              .maxStreams(maxConcurrency.toLong)
              .healthCheckPingPeriod(healthCheckPingPeriod.asJava)
              .build()
          )
          .protocol(Protocol.HTTP2)
          .build()
      }
    }

}
