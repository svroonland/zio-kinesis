package nl.vroste.zio.kinesis

import software.amazon.awssdk.services.cloudwatch.{ CloudWatchAsyncClient, CloudWatchAsyncClientBuilder }
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient, DynamoDbAsyncClientBuilder }
import software.amazon.awssdk.services.kinesis.{ KinesisAsyncClient, KinesisAsyncClientBuilder }
import zio.{ Has, ZIO, ZLayer, ZManaged }

package object client {

  type AdminClient     = Has[AdminClient.Service]
  type Client          = Has[Client.Service]
  type DynamicConsumer = Has[DynamicConsumer.Service]

  def kinesisAsyncClientLayer(
    builder: KinesisAsyncClientBuilder = KinesisAsyncClient.builder
  ): ZLayer[Any, Throwable, Has[KinesisAsyncClient]] =
    ZManaged.fromAutoCloseable(ZIO.effect(builder.build)).toLayer

  def cloudWatchAsyncClientLayer(
    builder: CloudWatchAsyncClientBuilder = CloudWatchAsyncClient.builder
  ): ZLayer[Any, Throwable, Has[CloudWatchAsyncClient]] =
    ZManaged.fromAutoCloseable(ZIO.effect(builder.build)).toLayer

  def dynamoDbAsyncClientLayer(
    builder: DynamoDbAsyncClientBuilder = DynamoDbAsyncClient.builder
  ): ZLayer[Any, Throwable, Has[DynamoDbAsyncClient]] =
    ZManaged.fromAutoCloseable(ZIO.effect(builder.build)).toLayer

}
