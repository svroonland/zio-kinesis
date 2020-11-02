package nl.vroste.zio.kinesis

import io.github.vigoo.zioaws.cloudwatch.CloudWatch
import io.github.vigoo.zioaws.core.config
import io.github.vigoo.zioaws.core.config.AwsConfig
import io.github.vigoo.zioaws.core.httpclient.HttpClient
import io.github.vigoo.zioaws.dynamodb.DynamoDb
import io.github.vigoo.zioaws.kinesis.Kinesis
import io.github.vigoo.zioaws.{ cloudwatch, dynamodb, kinesis }
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClientBuilder
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder
import zio.{ Has, ZLayer }

package object client {
  type DynamicConsumer   = Has[DynamicConsumer.Service]
  type HttpClientBuilder = Has[HttpClientBuilder.Service]

  def kinesisAsyncClientLayer(
    build: KinesisAsyncClientBuilder => KinesisAsyncClientBuilder = identity
  ): ZLayer[HttpClientBuilder, Throwable, Kinesis] =
    ZLayer.fromServiceManaged { httpClientBuilder =>
      kinesis
        .managed(
          build(_)
            .overrideConfiguration(ClientOverrideConfiguration.builder().retryPolicy(RetryPolicy.none()).build())
        )
        .provideLayer(createConfigLayer(httpClientBuilder, true))
    }

  def cloudWatchAsyncClientLayer(
    build: CloudWatchAsyncClientBuilder => CloudWatchAsyncClientBuilder = identity
  ): ZLayer[HttpClientBuilder, Throwable, CloudWatch] =
    ZLayer.fromServiceManaged { httpClientBuilder =>
      cloudwatch
        .managed(
          build(_).overrideConfiguration(ClientOverrideConfiguration.builder().retryPolicy(RetryPolicy.none()).build())
        )
        .provideLayer(createConfigLayer(httpClientBuilder, false))
    }

  def dynamoDbAsyncClientLayer(
    build: DynamoDbAsyncClientBuilder => DynamoDbAsyncClientBuilder = identity
  ): ZLayer[HttpClientBuilder, Throwable, DynamoDb] =
    ZLayer.fromServiceManaged { httpClientBuilder =>
      dynamodb
        .managed(
          build(_).overrideConfiguration(ClientOverrideConfiguration.builder().retryPolicy(RetryPolicy.none()).build())
        )
        .provideLayer(createConfigLayer(httpClientBuilder, false))
    }

  private def createConfigLayer(
    httpClientBuilder: HttpClientBuilder.Service,
    http2Supported: Boolean
  ): ZLayer[Any, Throwable, AwsConfig] =
    httpClientBuilder
      .createSdkHttpClient(http2Supported)
      .map { httpClient =>
        new HttpClient.Service {
          override val client: SdkAsyncHttpClient = httpClient
        }
      }
      .toLayer >>> config.default

  val sdkClients: ZLayer[HttpClientBuilder, Throwable, Kinesis with CloudWatch with DynamoDb] =
    kinesisAsyncClientLayer() ++ cloudWatchAsyncClientLayer() ++ dynamoDbAsyncClientLayer()

  val defaultEnvironment: ZLayer[Any, Throwable, Kinesis with CloudWatch with DynamoDb] =
    HttpClientBuilder.make() >>> sdkClients
}
