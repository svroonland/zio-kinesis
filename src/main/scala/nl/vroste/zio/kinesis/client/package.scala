package nl.vroste.zio.kinesis

import software.amazon.awssdk.http.nio.netty.{ Http2Configuration, NettyNioAsyncHttpClient }
import software.amazon.awssdk.services.cloudwatch.{ CloudWatchAsyncClient, CloudWatchAsyncClientBuilder }
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient, DynamoDbAsyncClientBuilder }
import software.amazon.awssdk.services.kinesis.{ KinesisAsyncClient, KinesisAsyncClientBuilder }
import zio.duration._
import zio.{ Has, ZIO, ZLayer, ZManaged }

package object client {

  type AdminClient     = Has[AdminClient.Service]
  type Client          = Has[Client.Service]
  type DynamicConsumer = Has[DynamicConsumer.Service]
  type HttpClient      = Has[HttpClient.Service]

  def kinesisAsyncClientLayer(
    build: KinesisAsyncClientBuilder => KinesisAsyncClient = _.build()
  ): ZLayer[HttpClient, Throwable, Has[KinesisAsyncClient]] =
    ZLayer.fromServiceManaged { httpClient =>
      httpClient.createSdkHttpClient(http2Supported = true).flatMap { client =>
        ZManaged.fromAutoCloseable(ZIO.effect(build(KinesisAsyncClient.builder().httpClient(client))))
      }
    }

  def cloudWatchAsyncClientLayer(
    build: CloudWatchAsyncClientBuilder => CloudWatchAsyncClient = _.build()
  ): ZLayer[HttpClient, Throwable, Has[CloudWatchAsyncClient]] =
    ZLayer.fromServiceManaged { httpClient =>
      httpClient.createSdkHttpClient(http2Supported = false).flatMap { client =>
        ZManaged.fromAutoCloseable(ZIO.effect(build(CloudWatchAsyncClient.builder().httpClient(client))))
      }
    }

  def dynamoDbAsyncClientLayer(
    build: DynamoDbAsyncClientBuilder => DynamoDbAsyncClient = _.build()
  ): ZLayer[HttpClient, Throwable, Has[DynamoDbAsyncClient]] =
    ZLayer.fromServiceManaged { httpClient =>
      // DynamoDB does not support HTTP 2
      httpClient.createSdkHttpClient(http2Supported = false).flatMap { client =>
        ZManaged.fromAutoCloseable(ZIO.effect(build(DynamoDbAsyncClient.builder().httpClient(client))))
      }
    }

  /**
   * Builder for SdkAsyncHttpClient that can be shared between the AWS SDK service clients
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
   * @return
   */
  def httpClientBuilder(
    maxConcurrency: Int = Int.MaxValue,
    initialWindowSize: Int = 512 * 1024, // 512 KB, see https://github.com/awslabs/amazon-kinesis-client/pull/706
    healthCheckPingPeriod: Duration = 10.seconds,
    maxPendingConnectionAcquires: Int = 10000,
    connectionAcquisitionTimeout: Duration = 30.seconds,
    readTimeout: Duration = 30.seconds
  ) =
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
}
