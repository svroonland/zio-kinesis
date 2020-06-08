package nl.vroste.zio.kinesis.client.native

import nl.vroste.zio.kinesis.client.Client
import nl.vroste.zio.kinesis.client.AdminClient
import zio.stream.ZStream
import nl.vroste.zio.kinesis.client.Client.ShardIteratorType
import nl.vroste.zio.kinesis.client.DynamicConsumer.Record
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.serde.Deserializer
import nl.vroste.zio.kinesis.client.Client.ConsumerRecord
import zio.ZIO
import zio.clock.Clock
import zio.Schedule
import zio.Ref
import software.amazon.awssdk.services.kinesis.model.Shard
import zio.duration._

case class Lease(shardId: String)

trait DynamoDbClient {
  def createLeaseTable(name: String): Task[Throwable]
}

trait LeaseCoordinator {
  def getLeases
  def updateLease
  def releaseLease
}

object Consumer {
  // TODO support lease retrieval and updating
  // TODO handle new shards
  // TODO support checkpointing
  // TODO handle rate limiting constraints?
  // TODO support KPL extended sequence numbering
  def shardedStream[R, T](
    client: Client,
    adminClient: AdminClient,
    streamName: String,
    applicationName: String,
    deserializer: Deserializer[R, T]
  ): ZStream[Clock, Throwable, (String, ZStream[R, Throwable, Record[T]])] = {

    def toRecord(
      shardId: String,
      r: ConsumerRecord
    ): ZIO[R, Throwable, Record[T]] =
      deserializer.deserialize(r.data.asByteBuffer()).map { data =>
        Record(
          shardId,
          r.sequenceNumber,
          r.approximateArrivalTimestamp,
          data,
          r.partitionKey,
          r.encryptionType,
          0,     // r.subSequenceNumber,
          "",    // r.explicitHashKey,
          false, //r.aggregated,
          checkpoint = ZIO.unit
        )
      }

    val shardRefreshInterval = 1.minute

    // This should be a stream that emits all new shards
    // TODO how to handle shard ends..?
    val currentShards: ZStream[Clock, Throwable, Shard] =
      ZStream.unwrap {
        // TODO somehow compare if we have it in our ref already and only emit when it's not yet
        for {
          activeShards <- Ref.make[List[Shard]](List.empty)
        } yield client
          .listShards(streamName)
          .repeat(Schedule.fixed(shardRefreshInterval))
          .mapConcatM { shard =>
            for {
              previousShards <- activeShards.getAndUpdate(_ :+ shard)
            } yield if (!previousShards.contains(shard)) List(shard) else Nil
          }
      }

    // TODO implement lease claiming. Something with hierarchical shards?
    val leasedShards: ZStream[Clock, Throwable, Shard] =
      // For each shard we should check if
      currentShards

    def shardStreamFrom(
      consumer: software.amazon.awssdk.services.kinesis.model.Consumer,
      shard: Shard,
      startingPosition: ShardIteratorType
    ): ZStream[Any, Throwable, ConsumerRecord] =
      ZStream.unwrap {
        for {
          currentPosition <- Ref.make[ShardIteratorType](startingPosition)
        } yield ZStream
          .fromEffect(currentPosition.get)
          .flatMap { pos =>
            // TODO is this enhanced fanout?
            client
              .subscribeToShard(
                consumer.consumerARN(),
                shard.shardId(),
                pos
              )
          }
          .tap(r => currentPosition.set(ShardIteratorType.AfterSequenceNumber(r.sequenceNumber)))
          .repeat(Schedule.forever) // Shard subscriptions get canceled after 5 minutes
      }

    ZStream.unwrapManaged {
      for {
        streamDescription <- adminClient.describeStream(streamName).toManaged_
        consumer          <- client.createConsumer(streamDescription.streamARN, applicationName)
      } yield leasedShards.map { shard =>
        // TODO default param, get from lease
        val startingPosition = ShardIteratorType.TrimHorizon

        val shardStream = shardStreamFrom(consumer, shard, startingPosition).mapChunksM { chunk =>
          chunk.mapM { case record => toRecord(shard.shardId(), record) }
        }

        // TODO end the shard stream when we have lost the lease for that shard

        (shard.shardId(), shardStream)
      }
    }
  }
}
