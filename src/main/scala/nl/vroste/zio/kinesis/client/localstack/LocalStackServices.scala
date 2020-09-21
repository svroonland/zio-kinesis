package nl.vroste.zio.kinesis.client.localstack

import java.net.URI

import nl.vroste.zio.kinesis.client.HttpClient
import software.amazon.awssdk.auth.credentials.{
  AwsBasicCredentials,
  AwsCredentialsProvider,
  StaticCredentialsProvider
}
import software.amazon.awssdk.core.SdkSystemSetting
import software.amazon.awssdk.http.SdkHttpConfigurationOption
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.utils.AttributeMap
import zio.duration._
import zio.{ Has, ZIO, ZLayer, ZManaged }

/**
 * Layers for connecting to a LocalStack (https://localstack.cloud/) environment on a local docker host
 */
object LocalStackServices {

  /**
   * A ZLayer containing implementations for CloudWatchAsyncClient, KinesisAsyncClient, DynamoDbAsyncClient configured
   * for localstack usage. Note localstack > 11.5 has introduced simplification of ports -
   * see https://github.com/localstack/localstack#user-content-announcements. These ports can be overriden using the url
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
  ): ZLayer[Any, Throwable, Has[CloudWatchAsyncClient] with Has[KinesisAsyncClient] with Has[DynamoDbAsyncClient]] = {
    val credsProvider: AwsCredentialsProvider =
      StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretAccessKey))

    val localHttpClient: ZLayer[Any, Nothing, HttpClient] =
      HttpClient.make(
        maxConcurrency = 25, // localstack 11.2 has hardcoded limit of 128 and we need to share with a few clients below
        maxPendingConnectionAcquires = 20,
        readTimeout = 10.seconds,
        allowHttp2 = false,  // Localstack does not support HTTP2
        build = _.connectionMaxIdleTime(10.seconds.asJava)
          .writeTimeout(10.seconds.asJava)
          .buildWithDefaults(
            AttributeMap.builder.put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, java.lang.Boolean.TRUE).build
          )
      )

    val kinesisAsyncClientLayer: ZLayer[HttpClient, Throwable, Has[KinesisAsyncClient]] =
      ZLayer.fromServiceManaged { httpClient =>
        httpClient.createSdkHttpClient().flatMap { sdkHttpClient =>
          ZManaged.fromAutoCloseable {
            ZIO.effect {
              System.setProperty(SdkSystemSetting.CBOR_ENABLED.property, "false")

              KinesisAsyncClient
                .builder()
                .credentialsProvider(credsProvider)
                .region(region)
                .endpointOverride(kinesisUri)
                .httpClient(sdkHttpClient)
                .build
            }
          }
        }
      }

    val dynamoDbClientLayer: ZLayer[HttpClient, Throwable, Has[DynamoDbAsyncClient]] =
      nl.vroste.zio.kinesis.client.dynamoDbAsyncClientLayer(
        _.credentialsProvider(credsProvider)
          .region(region)
          .endpointOverride(dynamoDbUri)
          .build()
      )

    val cloudWatchClientLayer: ZLayer[HttpClient, Throwable, Has[CloudWatchAsyncClient]] =
      nl.vroste.zio.kinesis.client.cloudWatchAsyncClientLayer(
        _.credentialsProvider(credsProvider)
          .region(region)
          .endpointOverride(cloudwatchUri)
          .build
      )

    localHttpClient >>> (cloudWatchClientLayer ++ kinesisAsyncClientLayer ++ dynamoDbClientLayer)
  }

}
