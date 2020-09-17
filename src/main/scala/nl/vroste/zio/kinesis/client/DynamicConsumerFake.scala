package nl.vroste.zio.kinesis.client

import java.nio.ByteBuffer

import nl.vroste.zio.kinesis.client.DynamicConsumer.Checkpointer
import nl.vroste.zio.kinesis.client.serde.{ Deserializer, Serializer }
import software.amazon.awssdk.services.kinesis.model.EncryptionType
import software.amazon.kinesis.common.InitialPositionInStreamExtended
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.stream.ZStream

private[client] class DynamicConsumerFake(
  shards: ZStream[Any, Throwable, (String, ZStream[Any, Throwable, ByteBuffer])],
  refCheckpointedList: Ref[Seq[Any]],
  clock: Clock.Service
) extends DynamicConsumer.Service {
  override def shardedStream[R, T](
    streamName: String,
    applicationName: String,
    deserializer: Deserializer[R, T],
    requestShutdown: UIO[Unit],
    initialPosition: InitialPositionInStreamExtended,
    isEnhancedFanOut: Boolean,
    leaseTableName: Option[String],
    workerIdentifier: String,
    maxShardBufferSize: Int
  ): ZStream[
    Blocking with R,
    Throwable,
    (String, ZStream[Blocking, Throwable, Record[T]], DynamicConsumer.Checkpointer)
  ] = {
    def record(shardName: String, i: Long, recData: T): UIO[Record[T]] =
      (for {
        dateTime <- clock.currentDateTime
      } yield new Record[T](
        sequenceNumber = s"$i",
        approximateArrivalTimestamp = dateTime.toInstant,
        data = recData,
        partitionKey = s"${shardName}_$i",
        encryptionType = EncryptionType.NONE,
        subSequenceNumber = Some(i),
        explicitHashKey = None,
        aggregated = false,
        shardId = shardName
      )).orDie

    shards.flatMap {
      case (shardName, stream) =>
        ZStream.fromEffect {
          ZIO.environment[R with Blocking].flatMap { env =>
            CheckpointerFake.make(refCheckpointedList).map { checkpointer =>
              (
                shardName,
                stream.zipWithIndex.mapM {
                  case (byteBuffer, i) =>
                    deserializer.deserialize(byteBuffer).flatMap(record(shardName, i, _)).provide(env)
                },
                checkpointer
              )
            }
          }
        }
    }

  }
}

object CheckpointerFake {
  def make(refCheckpointedList: Ref[Seq[_]]): Task[Checkpointer] =
    for {
      latestStaged <- Ref.make[Option[Record[_]]](None)
    } yield new DynamicConsumer.Checkpointer {
      override private[client] def peek: UIO[Option[Record[_]]] = latestStaged.get

      override def stage(r: Record[_]): UIO[Unit] = latestStaged.set(Some(r))

      override def checkpoint: ZIO[Blocking, Throwable, Unit] =
        ZIO.succeed(println(s">>>>>>>>> 1 about to checkpoint $latestStaged")) *>
          latestStaged.get.flatMap {
            case Some(record) =>
              ZIO.succeed(println(s">>>>>>>>> 2 about to checkpoint $record")) *>
                refCheckpointedList.update(seq => seq :+ record) *>
                latestStaged.update {
                  case Some(r) if r == record => None
                  case r                      => r // A newer record may have been staged by now
                }
            case None         => UIO.unit
          }
    }
}

object DynamicConsumerFake {

  def shardsFromIterables[R, T](
    serializer: Serializer[R, T],
    lists: List[T]*
  ): ZStream[Any, Nothing, (String, ZStream[R, Throwable, ByteBuffer])] = {
    val listOfShards = lists.zipWithIndex.map {
      case (xs, i) => (s"shard$i", ZStream.fromIterable(xs).mapM(serializer.serialize))
    }
    ZStream.fromIterable(listOfShards)
  }

  def shardsFromStreams[R, T](
    serializer: Serializer[R, T],
    streams: ZStream[R, Throwable, T]*
  ): ZStream[Any, Nothing, (String, ZStream[R, Throwable, ByteBuffer])] = {
    val listOfShards = streams.zipWithIndex.map {
      case (stream, i) => (s"shard$i", stream.mapM(serializer.serialize))
    }
    ZStream.fromIterable(listOfShards)
  }

}
