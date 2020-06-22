package nl.vroste.zio.kinesis.client

import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import zio.{ Has, ZIO, ZLayer, ZManaged }

object DefaultClientsLayers {

  def kinesisAsyncClientLayer: ZLayer[Any, Throwable, Has[KinesisAsyncClient]] =
    ZLayer.fromManaged(ZManaged.fromAutoCloseable {
      ZIO.effect(
        KinesisAsyncClient.builder.build
      )
    })

  def cloudWatchAsyncClientLayer: ZLayer[Any, Throwable, Has[CloudWatchAsyncClient]] =
    ZLayer.fromManaged(ZManaged.fromAutoCloseable {
      ZIO.effect(
        CloudWatchAsyncClient.builder.build
      )
    })

  def dynamoDbAsyncClientLayer: ZLayer[Any, Throwable, Has[DynamoDbAsyncClient]] =
    ZLayer.fromManaged(ZManaged.fromAutoCloseable {
      ZIO.effect(
        DynamoDbAsyncClient.builder.build
      )
    })

}
