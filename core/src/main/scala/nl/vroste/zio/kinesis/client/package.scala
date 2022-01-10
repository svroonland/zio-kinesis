package nl.vroste.zio.kinesis

import zio.aws.cloudwatch.CloudWatch
import zio.aws.core.config.AwsConfig
import zio.aws.core.httpclient
import zio.aws.core.httpclient.HttpClient
import zio.aws.dynamodb.DynamoDb
import zio.aws.kinesis.Kinesis
import zio.aws.{ cloudwatch, dynamodb, kinesis }
import software.amazon.awssdk.awscore.client.builder.{ AwsAsyncClientBuilder, AwsClientBuilder }
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClientBuilder
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder
import zio.{ Task, ZIO, ZLayer }

package object client {
  def kinesisAsyncClientLayer(
    build: KinesisAsyncClientBuilder => KinesisAsyncClientBuilder = identity
  ): ZLayer[AwsConfig, Throwable, Kinesis] =
    Kinesis.customized(build)

  def cloudWatchAsyncClientLayer(
    build: CloudWatchAsyncClientBuilder => CloudWatchAsyncClientBuilder = identity
  ): ZLayer[AwsConfig, Throwable, CloudWatch] =
    CloudWatch.customized(build)

  def dynamoDbAsyncClientLayer(
    build: DynamoDbAsyncClientBuilder => DynamoDbAsyncClientBuilder = identity
  ): ZLayer[AwsConfig, Throwable, DynamoDb] =
    DynamoDb.customized(build)

  val sdkClientsLayer: ZLayer[AwsConfig, Throwable, Kinesis with CloudWatch with DynamoDb] =
    kinesisAsyncClientLayer() ++ cloudWatchAsyncClientLayer() ++ dynamoDbAsyncClientLayer()

  val customConfig: ZLayer[HttpClient, Nothing, AwsConfig] =
    ZLayer.succeed {
      new AwsConfig {
        override def configure[Client, Builder <: AwsClientBuilder[Builder, Client]](builder: Builder): Task[Builder] =
          Task {
            builder.overrideConfiguration(ClientOverrideConfiguration.builder().retryPolicy(RetryPolicy.none()).build())
          }

        override def configureHttpClient[Client, Builder <: AwsAsyncClientBuilder[Builder, Client]](
          builder: Builder,
          serviceCaps: httpclient.ServiceHttpCapabilities
        ): Task[Builder] = ZIO.succeed(builder)
      }
    }

  val defaultAwsLayer: ZLayer[Any, Throwable, Kinesis with CloudWatch with DynamoDb] =
    HttpClientBuilder.make() >>> customConfig >>> sdkClientsLayer
}
