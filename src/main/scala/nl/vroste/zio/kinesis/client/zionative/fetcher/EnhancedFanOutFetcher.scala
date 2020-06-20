package nl.vroste.zio.kinesis.client.zionative.fetcher

import nl.vroste.zio.kinesis.client.AdminClient.StreamDescription
import nl.vroste.zio.kinesis.client.Client
import nl.vroste.zio.kinesis.client.Client.ShardIteratorType
import nl.vroste.zio.kinesis.client.zionative.{ Consumer, Fetcher }
import zio.{ Ref, Schedule, ZIO, ZManaged }
import zio.clock.Clock
import zio.stream.ZStream
import zio.duration._
import zio.Has

object EnhancedFanOutFetcher {
  def make(
    streamDescription: StreamDescription,
    applicationName: String
  ): ZManaged[Clock with Has[Client], Throwable, Fetcher] =
    for {
      client   <- ZIO.service[Client].toManaged_
      consumer <- client.createConsumer(streamDescription.streamARN, applicationName)
    } yield Fetcher { (shard, startingPosition) =>
      ZStream.unwrap {
        for {
          currentPosition <- Ref.make[ShardIteratorType](startingPosition)
          stream           = ZStream
                     .fromEffect(currentPosition.get)
                     .flatMap { pos =>
                       client
                         .subscribeToShard(
                           consumer.consumerARN(),
                           shard.shardId(),
                           pos
                         )
                     }
                     .tap(r => currentPosition.set(ShardIteratorType.AfterSequenceNumber(r.sequenceNumber)))
                     .repeat(Schedule.forever) // Shard subscriptions get canceled after 5 minutes

        } yield stream.catchSome(
          Consumer.isThrottlingException.andThen(_ => ZStream.unwrap(ZIO.sleep(1.second).as(stream)))
        ) // TODO this should be replaced with a ZStream#retry with a proper exponential backoff scheme
      }
    }
}
