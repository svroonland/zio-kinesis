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
import zio.{ Has, ZIO, ZLayer, ZManaged }

object LocalStackLayers {

  private val region: Region          = Region.of("us-east-1")
  private val kinesisUri: URI         = URI.create("http://localhost:4568")
  private val cloudwatchUri: URI      = URI.create("http://localhost:4582")
  private val dynamoDbUri: URI        = URI.create("http://localhost:4569")
  private val accessKey: String       = "dummy-key"
  private val secretAccessKey: String = "dummy-key"

  val credsProvider: AwsCredentialsProvider =
    StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretAccessKey))

  val localHttpClient: SdkAsyncHttpClient = {
    import software.amazon.awssdk.http.Protocol
    import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
    NettyNioAsyncHttpClient
      .builder()
      .protocol(Protocol.HTTP1_1)
      .buildWithDefaults(
        AttributeMap.builder.put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, java.lang.Boolean.TRUE).build
      )
  }

  val kinesisAsyncClientLayer: ZLayer[Any, Throwable, Has[KinesisAsyncClient]] =
    ZLayer.fromManaged(ZManaged.fromAutoCloseable {
      ZIO.effect(
        {
          System.setProperty(SdkSystemSetting.CBOR_ENABLED.property, "false")

          KinesisAsyncClient
            .builder()
            .credentialsProvider(credsProvider)
            .region(region)
            .endpointOverride(kinesisUri)
        }.httpClient(localHttpClient).build
      )
    })

  val dynamoDbClientLayer: ZLayer[Any, Throwable, Has[DynamoDbAsyncClient]] =
    ZLayer.fromManaged(ZManaged.fromAutoCloseable {
      ZIO.effect(
        DynamoDbAsyncClient
          .builder()
          .credentialsProvider(credsProvider)
          .region(region)
          .endpointOverride(dynamoDbUri)
          .build
      )
    })

  val cloudWatchClientLayer: ZLayer[Any, Throwable, Has[CloudWatchAsyncClient]] =
    ZLayer.fromManaged(ZManaged.fromAutoCloseable {
      ZIO.effect(
        CloudWatchAsyncClient
          .builder()
          .credentialsProvider(credsProvider)
          .region(region)
          .endpointOverride(cloudwatchUri)
          .build
      )
    })

  val dynamicConsumerLayer: ZLayer[Any, Throwable, Has[DynamicConsumer.Service]] =
    kinesisAsyncClientLayer ++ dynamoDbClientLayer ++ cloudWatchClientLayer >>> DynamicConsumer.live

}
