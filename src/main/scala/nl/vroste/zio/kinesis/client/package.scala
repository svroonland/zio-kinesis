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
    build: KinesisAsyncClientBuilder => KinesisAsyncClient = _.build()
  ): ZLayer[Has[SdkAsyncHttpClient], Throwable, Has[KinesisAsyncClient]] =
    ZLayer.fromServiceManaged { (httpClient: SdkAsyncHttpClient) =>
      ZManaged.fromAutoCloseable(ZIO.effect(build(KinesisAsyncClient.builder().httpClient(httpClient))))
    }

  def cloudWatchAsyncClientLayer(
    build: CloudWatchAsyncClientBuilder => CloudWatchAsyncClient = _.build()
  ): ZLayer[Has[SdkAsyncHttpClient], Throwable, Has[CloudWatchAsyncClient]] =
    ZLayer.fromServiceManaged { (httpClient: SdkAsyncHttpClient) =>
      ZManaged.fromAutoCloseable(ZIO.effect(build(CloudWatchAsyncClient.builder().httpClient(httpClient))))
    }

  def dynamoDbAsyncClientLayer(
    build: DynamoDbAsyncClientBuilder => DynamoDbAsyncClient = _.build()
  ): ZLayer[Has[SdkAsyncHttpClient], Throwable, Has[DynamoDbAsyncClient]] =
    ZLayer.fromServiceManaged { (httpClient: SdkAsyncHttpClient) =>
      ZManaged.fromAutoCloseable(ZIO.effect(build(DynamoDbAsyncClient.builder().httpClient(httpClient))))
    }

  /**
   * ZLayer for SdkAsyncHttpClient that can be shared between the AWS SDK service clients
   *
   * Some settings' defaults are recommended settings for Kinesis streaming using HTTP 2
   *
   * @param maxConcurrency Maximum concurrent connections.
   *                       Recommended to set higher than the amount of shards you expect to process
   * @param initialWindowSize
   * @param healthCheckPingPeriod
   * @param maxPendingConnectionAcquires
   * @param connectionAcquisitionTimeout
   * @param readTimeout
   * @param build Custom build steps
   * @return
   */
  def httpClientLayer(
    maxConcurrency: Int = Int.MaxValue,
    initialWindowSize: Int = 512 * 1024,   // 512 KB, see https://github.com/awslabs/amazon-kinesis-client/pull/706
    healthCheckPingPeriod: Duration = 10.seconds,
    maxPendingConnectionAcquires: Int = 10000,
    connectionAcquisitionTimeout: Duration = 30.seconds,
    readTimeout: Duration = 30.seconds,
    protocol: Protocol = Protocol.HTTP1_1, // TODO how to support both?
    build: NettyNioAsyncHttpClient.Builder => SdkAsyncHttpClient = _.build()
  ): ZLayer[Any, Throwable, Has[SdkAsyncHttpClient]] =
    ZLayer.fromManaged {
      ZManaged.fromAutoCloseable {
        ZIO.effect {
          build(
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
              .protocol(protocol)
          )
        }
      }
    }

}
