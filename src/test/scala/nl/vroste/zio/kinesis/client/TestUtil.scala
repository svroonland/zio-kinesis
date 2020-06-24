package nl.vroste.zio.kinesis.client
import software.amazon.awssdk.services.kinesis.model.{ ResourceInUseException, ResourceNotFoundException }
import zio.clock.Clock
import zio.{ Schedule, ZIO, ZManaged }
import zio.console.{ putStrLn, Console }
import zio.duration._
import zio.ZLayer
import zio.Has
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

object TestUtil {
  def createStream(streamName: String, nrShards: Int): ZManaged[Console with Has[AdminClient], Throwable, Unit] =
    for {
      adminClient <- ZManaged.service[AdminClient]
      _           <- adminClient
             .createStream(streamName, nrShards)
             .catchSome {
               case _: ResourceInUseException =>
                 putStrLn("Stream already exists")
             }
             .toManaged { _ =>
               adminClient
                 .deleteStream(streamName, enforceConsumerDeletion = true)
                 .catchSome {
                   case _: ResourceNotFoundException => ZIO.unit
                 }
                 .orDie
             }
    } yield ()

  def createStreamUnmanaged(streamName: String, nrShards: Int) =
    ZManaged
      .service[AdminClient]
      .use(
        _.createStream(streamName, nrShards).catchSome {
          case _: ResourceInUseException =>
            putStrLn("Stream already exists")
        }
      )

  val retryOnResourceNotFound: Schedule[Clock, Throwable, ((Throwable, Int), Duration)] =
    Schedule.doWhile[Throwable] {
      case _: ResourceNotFoundException => true
      case _                            => false
    } &&
      Schedule.recurs(5) &&
      Schedule.exponential(2.second)

  // Temporary until proper implementation of ZLayer
  object Layers {
    val kinesisAsyncClient: ZLayer[Any, Throwable, Has[KinesisAsyncClient]] =
      ZLayer.fromManaged(
        ZManaged.fromAutoCloseable(ZIO.effect(LocalStackDynamicConsumer.kinesisAsyncClientBuilder.build()))
      )

    val adminClient: ZLayer[Has[KinesisAsyncClient], Nothing, Has[AdminClient]] =
      ZLayer.fromService(AdminClient.fromAsyncClient(_))

    val client: ZLayer[Has[KinesisAsyncClient], Nothing, Has[Client]] = ZLayer.fromService(Client.fromAsyncClient(_))

    val dynamo: ZLayer[Any, Throwable, Has[DynamoDbAsyncClient]] = ZLayer.fromManaged(
      ZManaged.fromAutoCloseable(ZIO.effect(LocalStackDynamicConsumer.dynamoDbClientBuilder.build()))
    )
  }

}
