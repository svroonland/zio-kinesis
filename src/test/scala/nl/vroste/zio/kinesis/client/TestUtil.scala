package nl.vroste.zio.kinesis.client
import software.amazon.awssdk.services.kinesis.model.{ ResourceInUseException, ResourceNotFoundException }
import zio.clock.Clock
import zio.{ Schedule, ZIO, ZManaged }
import zio.console.{ putStrLn, Console }
import zio.duration._

object TestUtil {
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

  def createStreamUnmanaged(streamName: String, nrShards: Int) =
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
