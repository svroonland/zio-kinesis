package nl.vroste.zio.kinesis.client

import nl.vroste.zio.kinesis.client.AdminClient2.AdminClient2
import software.amazon.awssdk.services.kinesis.model.{ ResourceInUseException, ResourceNotFoundException }
import zio.clock.Clock
import zio.console.{ putStrLn, Console }
import zio.duration._
import zio.{ Schedule, ZIO, ZManaged }

object TestUtil {

  def createStream2(streamName: String, nrShards: Int): ZManaged[Console with AdminClient2, Throwable, Unit] =
    for {
      adminClient <- ZManaged.service[AdminClient2.Service]
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

  def createStream(streamName: String, nrShards: Int): ZManaged[Console, Throwable, Unit] =
    for {
      adminClient <- AdminClient.build(LocalStackClients.kinesisAsyncClientBuilder)
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

  def createStreamUnmanaged2(streamName: String, nrShards: Int): ZIO[Console with AdminClient2, Throwable, Unit] =
    for {
      adminClient <- ZIO.service[AdminClient2.Service]
      _           <- adminClient.createStream(streamName, nrShards).catchSome {
             case _: ResourceInUseException =>
               putStrLn("Stream already exists")
           }
    } yield ()

  def createStreamUnmanaged(streamName: String, nrShards: Int): ZIO[Console, Throwable, Unit] =
    AdminClient
      .build(LocalStackClients.kinesisAsyncClientBuilder)
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
}
