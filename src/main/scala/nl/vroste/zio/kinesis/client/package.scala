package nl.vroste.zio.kinesis

import io.github.vigoo.zioaws.cloudwatch.CloudWatch
import io.github.vigoo.zioaws.core.config
import io.github.vigoo.zioaws.core.config.AwsConfig
import io.github.vigoo.zioaws.dynamodb.DynamoDb
import io.github.vigoo.zioaws.kinesis.Kinesis
import io.github.vigoo.zioaws.{ cloudwatch, dynamodb, kinesis }
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClientBuilder
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder
import zio.{ Has, ZLayer }

package object client {
  type DynamicConsumer = Has[DynamicConsumer.Service]

  @deprecated("Use io.github.vigoo.zioaws.kinesis.live or io.github.vigoo.zioaws.kinesis.customized(builder)", "0.12")
  def kinesisAsyncClientLayer(
    build: KinesisAsyncClientBuilder => KinesisAsyncClientBuilder = identity
  ): ZLayer[AwsConfig, Throwable, Kinesis] =
    kinesis.customized(
      build(_).overrideConfiguration(ClientOverrideConfiguration.builder().retryPolicy(RetryPolicy.none()).build())
    )

  @deprecated(
    "Use io.github.vigoo.zioaws.cloudwatch.live or io.github.vigoo.zioaws.cloudwatch.customized(builder)",
    "0.12"
  )
  def cloudWatchAsyncClientLayer(
    build: CloudWatchAsyncClientBuilder => CloudWatchAsyncClientBuilder = identity
  ): ZLayer[AwsConfig, Throwable, CloudWatch] =
    cloudwatch.customized(
      build(_).overrideConfiguration(ClientOverrideConfiguration.builder().retryPolicy(RetryPolicy.none()).build())
    )

  @deprecated("Use io.github.vigoo.zioaws.dynamodb.live or io.github.vigoo.zioaws.dynamodb.customized(builder)", "0.12")
  def dynamoDbAsyncClientLayer(
    build: DynamoDbAsyncClientBuilder => DynamoDbAsyncClientBuilder = identity
  ): ZLayer[AwsConfig, Throwable, DynamoDb] =
    dynamodb.customized(
      build(_).overrideConfiguration(ClientOverrideConfiguration.builder().retryPolicy(RetryPolicy.none()).build())
    )

  val sdkClients: ZLayer[AwsConfig, Throwable, Kinesis with CloudWatch with DynamoDb] =
    kinesis.live ++ cloudwatch.live ++ dynamodb.live

  val defaultEnvironment: ZLayer[Any, Throwable, Kinesis with CloudWatch with DynamoDb] =
    HttpClientBuilder.make() >>> config.default >>> sdkClients
}
