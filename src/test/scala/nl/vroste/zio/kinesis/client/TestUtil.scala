package nl.vroste.zio.kinesis.client

import software.amazon.awssdk.services.kinesis.model.{ ResourceInUseException, ResourceNotFoundException, Shard }
import zio.clock.Clock
import zio.console.{ putStrLn, Console }
import zio.duration._
import zio.{ Chunk, Schedule, ZIO, ZManaged }

object TestUtil {

  def withStream[R, A](name: String, shards: Int)(
    f: ZIO[R, Throwable, A]
  ): ZIO[AdminClient with Client with Clock with Console with R, Throwable, A] =
    TestUtil
      .createStream(name, shards)
      .tapM(_ => getShards(name))
      .use_(f)

  def getShards(name: String): ZIO[Client with Clock, Throwable, Chunk[Shard]]                                        =
    ZIO
      .service[Client.Service]
      .flatMap(_.listShards(name).runCollect)
      .filterOrElse(_.nonEmpty)(_ => getShards(name).delay(1.second))
      .catchSome { case _: ResourceInUseException => getShards(name).delay(1.second) }

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
    ((if (batchIndex == 1) 1 else (batchIndex - 1) * batchSize) to (batchSize * batchIndex) - 1)

}
