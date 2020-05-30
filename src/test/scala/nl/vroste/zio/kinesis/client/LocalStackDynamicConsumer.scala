package nl.vroste.zio.kinesis.client

import java.net.URI
import java.util.UUID

import nl.vroste.zio.kinesis.client.serde.Deserializer
import software.amazon.awssdk.auth.credentials.{
  AwsBasicCredentials,
  AwsCredentialsProvider,
  StaticCredentialsProvider
}
import software.amazon.awssdk.core.SdkSystemSetting
import software.amazon.awssdk.http.SdkHttpConfigurationOption
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.cloudwatch.{ CloudWatchAsyncClient, CloudWatchAsyncClientBuilder }
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient, DynamoDbAsyncClientBuilder }
import software.amazon.awssdk.services.kinesis.{ KinesisAsyncClient, KinesisAsyncClientBuilder }
import software.amazon.awssdk.utils.AttributeMap
import zio.blocking.Blocking
import zio.stream.ZStream

object LocalStackDynamicConsumer {

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

  val kinesisAsyncClientBuilder: KinesisAsyncClientBuilder = {
    System.setProperty(SdkSystemSetting.CBOR_ENABLED.property, "false")
    KinesisAsyncClient
      .builder()
      .credentialsProvider(credsProvider)
      .region(region)
      .endpointOverride(kinesisUri)
  }.httpClient(localHttpClient)

  val dynamoDbClientBuilder: DynamoDbAsyncClientBuilder =
    DynamoDbAsyncClient
      .builder()
      .credentialsProvider(credsProvider)
      .region(region)
      .endpointOverride(dynamoDbUri)

  val cloudWatchClientBuilder: CloudWatchAsyncClientBuilder =
    CloudWatchAsyncClient
      .builder()
      .credentialsProvider(credsProvider)
      .region(region)
      .endpointOverride(cloudwatchUri)

  def shardedStream[R, T](
    streamName: String,
    applicationName: String,
    deserializer: Deserializer[R, T],
    workerIdentifier: String = UUID.randomUUID().toString
  ): ZStream[Blocking with R, Throwable, (String, ZStream[Any, Throwable, DynamicConsumer.Record[T]])] =
    DynamicConsumer.shardedStream(
      streamName,
      applicationName,
      deserializer,
      kinesisAsyncClientBuilder,
      cloudWatchClientBuilder,
      dynamoDbClientBuilder,
      isEnhancedFanOut = false,
      workerIdentifier = workerIdentifier
    )

}
