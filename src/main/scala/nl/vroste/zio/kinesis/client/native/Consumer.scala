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

object Consumer {
  // TODO support KPL subsquence numbering
  // TODO support checkpointing
  // TODO support subscribeToShard renewal
  // TODO handle new shards
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

    ZStream.unwrapManaged {
      for {
        streamDescription <- adminClient.describeStream(streamName).toManaged_
        consumer          <- client.createConsumer(streamDescription.streamARN, applicationName)
      } yield client.listShards(streamName).map { shard =>
        val startingPosition = ShardIteratorType.TrimHorizon
        val shardStream      = client
          .subscribeToShard(
            consumer.consumerARN(),
            shard.shardId(),
            startingPosition
          )
          .repeat(Schedule.forever)
          .mapChunksM { chunk =>
            chunk.mapM { case record => toRecord(shard.shardId(), record) }
          }

        (shard.shardId(), shardStream)
      }
    }
  }
}
