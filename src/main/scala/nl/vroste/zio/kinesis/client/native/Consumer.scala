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

    // This should be a stream that emits all new shards
    // TODO how to handle shard ends..?
    val currentShards: ZStream[Clock, Throwable, Shard] =
      ZStream.unwrap {
        for {
          activeShards <- Ref.make[List[Shard]](List.empty)
          _            <- UIO(println("Calling listShards"))
        } yield client
          .listShards(streamName)
          .repeat(Schedule.fixed(shardRefreshInterval))
          .mapConcatM { shard =>
            println(s"Checking active shard ${shard}")
            for {
              previousShards <- activeShards.getAndUpdate(_ :+ shard)
            } yield if (!previousShards.contains(shard)) List(shard) else Nil
          }
      }

    object EnhancedFanOut {

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

    object Polling {
      def shardStreamFrom(
        shard: Shard,
        startingPosition: ShardIteratorType,
        batchSize: Int,
        pollInterval: Duration
      ): ZStream[Clock, Throwable, ConsumerRecord] = {

        def toConsumerRecord(record: KinesisRecord): ConsumerRecord =
          ConsumerRecord(
            record.sequenceNumber(),
            record.approximateArrivalTimestamp(),
            record.data(),
            record.partitionKey(),
            record.encryptionType(),
            shard.shardId()
          )

        ZStream.unwrap {
          for {
            delayRef             <- Ref.make[Boolean](false)
            initialShardIterator <- client.getShardIterator(streamName, shard.shardId(), startingPosition)
            shardIterator        <- Ref.make[String](initialShardIterator)
          } yield ZStream.repeatEffectChunkOption {
            for {
              _               <- (UIO(println("s${shard.shardId()}: delaying poll")) *> ZIO.sleep(pollInterval)).whenM(delayRef.get)
              currentIterator <- shardIterator.get
              response        <- client.getRecords(currentIterator, batchSize).asSomeError
              records          = response.records.asScala.toList
              _                = println(s"${shard.shardId()}: Got ${records.size} records")
              _               <- delayRef.set(records.isEmpty)
              _               <- Option(response.nextShardIterator).map(shardIterator.set).getOrElse(ZIO.fail(None))
            } yield Chunk.fromIterable(records.map(toConsumerRecord))
          }
        }
      }
    }

    ZStream.unwrapManaged {
      for {
        streamDescription <- adminClient.describeStream(streamName).debug("desribeStream").toManaged_
        _                 <- UIO(println(s"Stream description: ${streamDescription}")).toManaged_
      } yield currentShards.map { shard =>
        val startingPosition = ShardIteratorType.TrimHorizon

        val shardStream = Polling.shardStreamFrom(shard, startingPosition, 100, 1.second).mapChunksM { chunk =>
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
}
