package nl.vroste.zio.kinesis.client.native

import nl.vroste.zio.kinesis.client.Client
import nl.vroste.zio.kinesis.client.AdminClient
import zio.stream.ZStream
import nl.vroste.zio.kinesis.client.Client.ShardIteratorType
import nl.vroste.zio.kinesis.client.DynamicConsumer.Record
import nl.vroste.zio.kinesis.client.DynamicConsumer.Checkpointer
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.serde.Deserializer
import nl.vroste.zio.kinesis.client.Client.ConsumerRecord
import zio.ZIO
import zio.clock.Clock
import zio.Schedule
import zio.Ref
import software.amazon.awssdk.services.kinesis.model.Shard
import zio.duration._
import zio.UIO

import scala.jdk.CollectionConverters._
import software.amazon.awssdk.services.kinesis.model.{ Record => KinesisRecord }
import zio.Chunk
import nl.vroste.zio.kinesis.client.native.FetchMode.Polling
import nl.vroste.zio.kinesis.client.native.FetchMode.EnhancedFanOut
import zio.ZManaged
import nl.vroste.zio.kinesis.client.AdminClient.StreamDescription

sealed trait FetchMode
object FetchMode {

  /**
   * Fetches data in a polling manner
   *
   * @param batchSize The maximum number of records to retrieve in one call to GetRecords. Note that Kinesis
   *        defines limits in terms of the maximum size in bytes of this call, so you need to take into account
   *        the distribution of data size of your records (i.e. avg and max).
   * @param backoff How long to wait after polling returned no new records
   */
  case class Polling(batchSize: Int = 100, backoff: Duration = 1.second) extends FetchMode

  /**
   * Fetch data using enhanced fanout
   */
  case object EnhancedFanOut extends FetchMode
}

private object EnhancedFanOutFetcher {
  def shardStream(
    client: Client,
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
}

private object PollingFetcher {
  def shardStream(
    client: Client,
    streamDescription: StreamDescription,
    shard: Shard,
    startingPosition: ShardIteratorType,
    config: FetchMode.Polling
  ): ZStream[Clock, Throwable, ConsumerRecord] =
    ZStream.unwrap {
      for {
        delayRef             <- Ref.make[Boolean](false)
        initialShardIterator <- client.getShardIterator(streamDescription.streamName, shard.shardId(), startingPosition)
        shardIterator        <- Ref.make[String](initialShardIterator)
      } yield ZStream.repeatEffectChunkOption {
        for {
          _               <- (UIO(println("s${shard.shardId()}: delaying poll")) *> ZIO.sleep(config.backoff)).whenM(delayRef.get)
          currentIterator <- shardIterator.get
          response        <- client.getRecords(currentIterator, config.batchSize).asSomeError
          records          = response.records.asScala.toList
          _                = println(s"${shard.shardId()}: Got ${records.size} records")
          _               <- delayRef.set(records.isEmpty)
          _               <- Option(response.nextShardIterator).map(shardIterator.set).getOrElse(ZIO.fail(None))
        } yield Chunk.fromIterable(records.map(Consumer.toConsumerRecord(_, shard.shardId())))
      }
    }

}

object Consumer {
  def shardedStream[R, T](
    client: Client,
    adminClient: AdminClient,
    streamName: String,
    applicationName: String,
    deserializer: Deserializer[R, T],
    fetchMode: FetchMode = FetchMode.Polling()
  ): ZStream[Clock, Throwable, (String, ZStream[R with Clock, Throwable, Record[T]], Checkpointer)] = {

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
          0,    // r.subSequenceNumber,
          "",   // r.explicitHashKey,
          false //r.aggregated,
        )
      }

    val shardRefreshInterval = 1.minute

    val currentShards: ZStream[Clock, Throwable, Shard] = client.listShards(streamName)

    trait Fetcher {
      def fetch(shard: Shard, startingPosition: ShardIteratorType): ZStream[Clock, Throwable, ConsumerRecord]
    }

    def makeFetcher(streamDescription: StreamDescription): ZManaged[Any, Throwable, Fetcher] =
      fetchMode match {
        case c @ Polling(batchSize, backoff) =>
          ZManaged.succeed((shard, startingPosition) =>
            PollingFetcher.shardStream(client, streamDescription, shard, startingPosition, c)
          )
        case EnhancedFanOut                  =>
          client
            .createConsumer(streamDescription.streamARN, applicationName)
            .map(consumer =>
              (shard, startingPosition) => EnhancedFanOutFetcher.shardStream(client, consumer, shard, startingPosition)
            )

      }

    ZStream.unwrapManaged {
      for {
        streamDescription <- adminClient.describeStream(streamName).debug("desribeStream").toManaged_
        _                 <- UIO(println(s"Stream description: ${streamDescription}")).toManaged_
        fetcher           <- makeFetcher(streamDescription)
      } yield currentShards.map { shard =>
        val startingPosition = ShardIteratorType.TrimHorizon

        val shardStream = fetcher.fetch(shard, startingPosition).mapChunksM { chunk =>
          chunk.mapM { case record => toRecord(shard.shardId(), record) }
        }

        (shard.shardId(), shardStream, dummyCheckpointer)
      }
    }
  }

  val dummyCheckpointer = new Checkpointer {
    override def checkpoint: ZIO[zio.blocking.Blocking, Throwable, Unit] = ZIO.unit
    override def stage(r: Record[_]): zio.UIO[Unit]                      = ZIO.unit
  }

  implicit class ZioDebugExtensions[R, E, A](z: ZIO[R, E, A]) {
    def debug(label: String): ZIO[R, E, A] = (UIO(println(s"${label}")) *> z) <* UIO(println(s"${label} complete"))
  }

  def toConsumerRecord(record: KinesisRecord, shardId: String): ConsumerRecord =
    ConsumerRecord(
      record.sequenceNumber(),
      record.approximateArrivalTimestamp(),
      record.data(),
      record.partitionKey(),
      record.encryptionType(),
      shardId
    )

}
