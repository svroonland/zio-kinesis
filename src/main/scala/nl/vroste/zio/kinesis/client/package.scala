package nl.vroste.zio.kinesis

import io.github.vigoo.zioaws.cloudwatch.CloudWatch
import io.github.vigoo.zioaws.core.config.AwsConfig
import io.github.vigoo.zioaws.dynamodb.DynamoDb
import io.github.vigoo.zioaws.kinesis.Kinesis
import io.github.vigoo.zioaws.{ cloudwatch, dynamodb, kinesis }
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClientBuilder
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder
import zio.{ Has, ZLayer }

package object client {
  type DynamicConsumer = Has[DynamicConsumer.Service]

  // TODO on http client
//  // We do our own retries for the purposes of logging and metrics
//  .overrideConfiguration(ClientOverrideConfiguration.builder().retryPolicy(RetryPolicy.none()).build())

  @deprecated("Use io.github.vigoo.zioaws.kinesis.live or io.github.vigoo.zioaws.kinesis.customized(builder)")
  def kinesisAsyncClientLayer(
    build: KinesisAsyncClientBuilder => KinesisAsyncClientBuilder = identity
  ): ZLayer[AwsConfig, Throwable, Kinesis] =
    kinesis.customized(build)

  @deprecated("Use io.github.vigoo.zioaws.cloudwatch.live or io.github.vigoo.zioaws.cloudwatch.customized(builder)")
  def cloudWatchAsyncClientLayer(
    build: CloudWatchAsyncClientBuilder => CloudWatchAsyncClientBuilder = identity
  ): ZLayer[AwsConfig, Throwable, CloudWatch] =
    cloudwatch.customized(build)

  @deprecated("Use io.github.vigoo.zioaws.dynamodb.live or io.github.vigoo.zioaws.dynamodb.customized(builder)")
  def dynamoDbAsyncClientLayer(
    build: DynamoDbAsyncClientBuilder => DynamoDbAsyncClientBuilder = identity
  ): ZLayer[AwsConfig, Throwable, DynamoDb] =
    dynamodb.customized(build)

  val sdkClients: ZLayer[AwsConfig, Throwable, Kinesis with CloudWatch with DynamoDb] =
    kinesis.live ++ cloudwatch.live ++ dynamodb.live
}
