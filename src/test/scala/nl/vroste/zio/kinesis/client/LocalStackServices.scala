package nl.vroste.zio.kinesis.client

import java.net.URI

import software.amazon.awssdk.auth.credentials.{
  AwsBasicCredentials,
  AwsCredentialsProvider,
  StaticCredentialsProvider
}
import software.amazon.awssdk.core.SdkSystemSetting
import software.amazon.awssdk.http.SdkHttpConfigurationOption
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
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

  private val region: Region          = Region.of("us-east-1")
  private val kinesisUri: URI         = URI.create("http://localhost:4568")
  private val cloudwatchUri: URI      = URI.create("http://localhost:4582")
  private val dynamoDbUri: URI        = URI.create("http://localhost:4569")
  private val accessKey: String       = "dummy-key"
  private val secretAccessKey: String = "dummy-key"

  val credsProvider: AwsCredentialsProvider =
    StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretAccessKey))

  val localHttpClient = {
    import software.amazon.awssdk.http.Protocol

    httpClientLayer(
      maxConcurrency = 25, // localstack 11.2 has hardcoded limit of 128
      maxPendingConnectionAcquires = 20,
      readTimeout = 10.seconds,
      build = _.protocol(Protocol.HTTP1_1)
        .connectionMaxIdleTime(10.seconds.asJava)
        .writeTimeout(10.seconds.asJava)
        .buildWithDefaults(
          AttributeMap.builder.put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, java.lang.Boolean.TRUE).build
        )
    )
  }

  val kinesisAsyncClientLayer: ZLayer[Has[SdkAsyncHttpClient], Throwable, Has[KinesisAsyncClient]] =
    ZLayer.fromServiceManaged { httpClient =>
      ZManaged.fromAutoCloseable {
        ZIO.effect {
          System.setProperty(SdkSystemSetting.CBOR_ENABLED.property, "false")

          KinesisAsyncClient
            .builder()
            .credentialsProvider(credsProvider)
            .region(region)
            .endpointOverride(kinesisUri)
            .httpClient(httpClient)
            .build
        }
      }
    }

  val dynamoDbClientLayer: ZLayer[Has[SdkAsyncHttpClient], Throwable, Has[DynamoDbAsyncClient]] =
    nl.vroste.zio.kinesis.client.dynamoDbAsyncClientLayer(
      _.credentialsProvider(credsProvider)
        .region(region)
        .endpointOverride(dynamoDbUri)
        .build()
    )

  val cloudWatchClientLayer: ZLayer[Has[SdkAsyncHttpClient], Throwable, Has[CloudWatchAsyncClient]] =
    nl.vroste.zio.kinesis.client.cloudWatchAsyncClientLayer(
      _.credentialsProvider(credsProvider)
        .region(region)
        .endpointOverride(cloudwatchUri)
        .build
    )

  val env = localHttpClient >>> (cloudWatchClientLayer ++ kinesisAsyncClientLayer ++ dynamoDbClientLayer)

  val dynamicConsumerLayer: ZLayer[Any, Throwable, Has[DynamicConsumer.Service]] =
    env >>> DynamicConsumer.live
}
