package nl.vroste.zio.kinesis.client.localstack

import java.net.URI

import io.github.vigoo.zioaws.cloudwatch.CloudWatch
import io.github.vigoo.zioaws.core.config.AwsConfig
import io.github.vigoo.zioaws.dynamodb.DynamoDb
import io.github.vigoo.zioaws.kinesis.Kinesis
import io.github.vigoo.zioaws.{ cloudwatch, dynamodb, kinesis }
import nl.vroste.zio.kinesis.client.HttpClientBuilder
import software.amazon.awssdk.auth.credentials.{
  AwsBasicCredentials,
  AwsCredentialsProvider,
  StaticCredentialsProvider
}
import software.amazon.awssdk.awscore.client.builder.{ AwsAsyncClientBuilder, AwsClientBuilder }
import software.amazon.awssdk.core.SdkSystemSetting
import software.amazon.awssdk.http.SdkHttpConfigurationOption
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.utils.AttributeMap
import zio.duration._
import zio.{ Has, Task, ZLayer }

/**
 * Layers for connecting to a LocalStack (https://localstack.cloud/) environment on a local docker host
 */
object LocalStackServices {

  /**
   * A ZLayer containing implementations for CloudWatchAsyncClient, KinesisAsyncClient, DynamoDbAsyncClient configured
   * for localstack usage. Note localstack > 11.5 has introduced simplification of ports -
   * see https://github.com/localstack/localstack#user-content-announcements. These ports can be overridden using the url
   * parameters below for earlier versions of localstack.
   * @param kinesisUri Defaults to `http://localhost:4566`
   * @param cloudwatchUri Defaults to `http://localhost:4566`
   * @param dynamoDbUri Defaults to `http://localhost:4566`
   * @param accessKey Defaults to `dummy-key`
   * @param secretAccessKey Defaults to `dummy-key`
   * @param region Defaults to `us-east-1`
   * @return A ZLayer containing implementations for CloudWatchAsyncClient, KinesisAsyncClient, DynamoDbAsyncClient
   *         configured for localstack usage
   */
  def localStackAwsLayer(
    kinesisUri: URI = URI.create("http://localhost:4566"),
    cloudwatchUri: URI = URI.create("http://localhost:4566"),
    dynamoDbUri: URI = URI.create("http://localhost:4566"),
    accessKey: String = "dummy-key",
    secretAccessKey: String = "dummy-key",
    region: Region = Region.of("us-east-1")
  ): ZLayer[Any, Throwable, CloudWatch with Kinesis with DynamoDb] = {
    val credsProvider: AwsCredentialsProvider =
      StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretAccessKey))

    val localHttpClient: ZLayer[Any, Throwable, HttpClientBuilder] =
      HttpClientBuilder
        .make(
          maxConcurrency =
            25,               // localstack 11.2 has hardcoded limit of 128 and we need to share with a few clients below
          maxPendingConnectionAcquires = 20,
          readTimeout = 10.seconds,
          allowHttp2 = false, // Localstack does not support HTTP2
          build = _.connectionMaxIdleTime(10.seconds.asJava)
            .writeTimeout(10.seconds.asJava)
            .buildWithDefaults(
              AttributeMap.builder.put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, java.lang.Boolean.TRUE).build
            )
        )

    val awsConfig: ZLayer[HttpClientBuilder, Throwable, AwsConfig] =
      ZLayer.fromServiceManaged { httpClientBuilder: HttpClientBuilder.Service =>
        httpClientBuilder.createSdkHttpClient(http2Supported = false).map { httpClient =>
          new AwsConfig.Service {
            override def configure[Client, Builder <: AwsClientBuilder[Builder, Client]](
              builder: Builder
            ): Task[Builder] =
              Task.succeed(
                builder
                  .credentialsProvider(credsProvider)
                  .region(region)
              )

            override def configureHttpClient[Client, Builder <: AwsAsyncClientBuilder[Builder, Client]](
              builder: Builder
            ): Task[Builder] = Task.succeed(builder.httpClient(httpClient))
          }
        }
      }

    val kinesisAsyncClientLayer: ZLayer[AwsConfig, Throwable, Kinesis] =
      kinesis.customized { builder =>
        System.setProperty(SdkSystemSetting.CBOR_ENABLED.property, "false")

        builder
          .endpointOverride(kinesisUri)
      }

    val dynamoDbClientLayer: ZLayer[AwsConfig, Throwable, DynamoDb] =
      dynamodb.customized(_.endpointOverride(dynamoDbUri))

    val cloudWatchClientLayer: ZLayer[AwsConfig, Throwable, CloudWatch] =
      cloudwatch.customized(_.endpointOverride(cloudwatchUri))

    val env: ZLayer[Any, Throwable, CloudWatch with Kinesis with DynamoDb] =
      localHttpClient >>> awsConfig >>> (cloudWatchClientLayer ++ kinesisAsyncClientLayer ++ dynamoDbClientLayer)

    env
  }
}
