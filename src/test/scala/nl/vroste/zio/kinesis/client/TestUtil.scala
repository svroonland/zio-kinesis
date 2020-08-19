package nl.vroste.zio.kinesis.client

import software.amazon.awssdk.services.kinesis.model.{ ResourceInUseException, ResourceNotFoundException }
import zio.clock.Clock
import zio.console.{ putStrLn, Console }
import zio.duration._
import zio.{ Schedule, ZIO, ZManaged }

object TestUtil {

  def createStream(streamName: String, nrShards: Int): ZManaged[Console with AdminClient with Clock, Throwable, Unit] =
    createStreamUnmanaged(streamName, nrShards).toManaged(_ =>
      ZIO
        .service[AdminClient.Service]
        .flatMap(_.deleteStream(streamName, enforceConsumerDeletion = true))
        .catchSome {
          case _: ResourceNotFoundException => ZIO.unit
        }
        .orDie
    )

  def createStreamUnmanaged(
    streamName: String,
    nrShards: Int
  ): ZIO[Console with AdminClient with Clock, Throwable, Unit] =
    for {
      adminClient <- ZIO.service[AdminClient.Service]
      _           <- adminClient
             .createStream(streamName, nrShards)
             .catchSome {
               case _: ResourceInUseException =>
                 putStrLn("Stream already exists")
             }
             .retry(Schedule.exponential(1.second) && Schedule.recurs(10))
    } yield ()

  val retryOnResourceNotFound: Schedule[Clock, Throwable, ((Throwable, Long), Duration)] =
    Schedule.recurWhile[Throwable] {
      case _: ResourceNotFoundException => true
      case _                            => false
    } &&
      Schedule.recurs(5) &&
      Schedule.exponential(2.second)

  def recordsForBatch(batchIndex: Int, batchSize: Int): Seq[Int] =
    ((if (batchIndex == 1) 1 else ((batchIndex - 1) * batchSize) + 1) to (batchSize * batchIndex))

}
