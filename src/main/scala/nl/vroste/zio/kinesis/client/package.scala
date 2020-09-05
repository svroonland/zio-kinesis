package nl.vroste.zio.kinesis

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.services.cloudwatch.{ CloudWatchAsyncClient, CloudWatchAsyncClientBuilder }
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient, DynamoDbAsyncClientBuilder }
import software.amazon.awssdk.services.kinesis.{ KinesisAsyncClient, KinesisAsyncClientBuilder }
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
        ZManaged.fromAutoCloseable(
          ZIO.effect(
            build(
              KinesisAsyncClient
                .builder()
                // We do our own retries for the purposes of logging and metrics
                .overrideConfiguration(ClientOverrideConfiguration.builder().retryPolicy(RetryPolicy.none()).build())
                .httpClient(client)
            )
          )
        )
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

  val sdkClientsLayer: ZLayer[HttpClient, Throwable, Has[KinesisAsyncClient] with Has[CloudWatchAsyncClient] with Has[
    DynamoDbAsyncClient
  ]] = kinesisAsyncClientLayer() ++ cloudWatchAsyncClientLayer() ++ dynamoDbAsyncClientLayer()

  val defaultAwsLayer
    : ZLayer[Any, Throwable, Has[KinesisAsyncClient] with Has[CloudWatchAsyncClient] with Has[DynamoDbAsyncClient]] =
    HttpClient.make() >>>
      sdkClientsLayer
}
