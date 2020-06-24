package nl.vroste.zio.kinesis.client.zionative.fetcher

import nl.vroste.zio.kinesis.client.AdminClient.StreamDescription
import nl.vroste.zio.kinesis.client.Client
import nl.vroste.zio.kinesis.client.Client.ShardIteratorType
import nl.vroste.zio.kinesis.client.zionative.{ Consumer, Fetcher }
import zio.{ Ref, Schedule, ZIO, ZManaged }
import zio.clock.Clock
import zio.stream.ZStream
import zio.UIO
import zio.duration._
import zio.Has
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException
import software.amazon.awssdk.services.kinesis.model.ConsumerStatus

object EnhancedFanOutFetcher {
  private def registerConsumerIfNotExists(streamARN: String, consumerName: String) =
    ZIO.service[Client].flatMap(_.registerStreamConsumer(streamARN, consumerName)).map(_.consumerARN()).catchSome {
      case e: ResourceInUseException =>
        // Consumer already exists, retrieve it
        ZIO
          .service[Client]
          .flatMap(_.describeStreamConsumer(streamARN, consumerName))
          .filterOrElse(_.consumerStatus() != ConsumerStatus.DELETING)(_ => ZIO.fail(e))
          .map(_.consumerARN())
    }

  // TODO doesn't really need to be managed anymore
  def make(
    streamDescription: StreamDescription,
    workerId: String
  ): ZManaged[Clock with Has[Client], Throwable, Fetcher] =
    for {
      client      <- ZIO.service[Client].toManaged_
      _            = println("Creating consumer")
      consumerARN <- registerConsumerIfNotExists(streamDescription.streamARN, workerId).toManaged_
    } yield Fetcher { (shard, startingPosition) =>
      ZStream.unwrap {
        for {
          currentPosition <- Ref.make[ShardIteratorType](startingPosition)
          stream           = ZStream
                     .fromEffect(currentPosition.get)
                     .flatMap { pos =>
                       println(s"Beginning enhanced fan out stream for shard ${shard}")
                       client
                         .subscribeToShard(
                           consumerARN,
                           shard.shardId(),
                           pos
                         )
                     }
                     .tap(r => currentPosition.set(ShardIteratorType.AfterSequenceNumber(r.sequenceNumber)))
                     .repeat(Schedule.forever) // Shard subscriptions get canceled after 5 minutes

        } yield stream.catchSome(
          Consumer.isThrottlingException.andThen(_ =>
            ZStream.unwrap(ZIO.sleep(1.second).tap(_ => UIO(println("Throttled!"))).as(stream))
          )
        ) // TODO this should be replaced with a ZStream#retry with a proper exponential backoff scheme
      }
    }
}
