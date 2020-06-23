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
    ZLayer.fromManaged(ZManaged.fromAutoCloseable {
      ZIO.effect(
        builder.build
      )
    })

  def cloudWatchAsyncClientLayer(
    builder: CloudWatchAsyncClientBuilder = CloudWatchAsyncClient.builder
  ): ZLayer[Any, Throwable, Has[CloudWatchAsyncClient]] =
    ZLayer.fromManaged(ZManaged.fromAutoCloseable {
      ZIO.effect(
        builder.build
      )
    })

  def dynamoDbAsyncClientLayer(
    builder: DynamoDbAsyncClientBuilder = DynamoDbAsyncClient.builder
  ): ZLayer[Any, Throwable, Has[DynamoDbAsyncClient]] =
    ZLayer.fromManaged(ZManaged.fromAutoCloseable {
      ZIO.effect(
        builder.build
      )
    })

}
