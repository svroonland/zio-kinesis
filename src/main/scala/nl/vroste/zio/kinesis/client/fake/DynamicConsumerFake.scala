package nl.vroste.zio.kinesis.client.fake

import java.nio.ByteBuffer

import nl.vroste.zio.kinesis.client.DynamicConsumer.Checkpointer
import nl.vroste.zio.kinesis.client.serde.{ Deserializer, Serializer }
import nl.vroste.zio.kinesis.client.{ DynamicConsumer, Record }
import software.amazon.awssdk.services.kinesis.model.EncryptionType
import software.amazon.kinesis.common.InitialPositionInStreamExtended
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.stream.ZStream

private[client] class DynamicConsumerFake[T](
  shards: ZStream[Any, Throwable, (String, ZStream[Any, Throwable, ByteBuffer])],
  refCheckpointedList: Ref[Seq[T]],
  clock: Clock.Service
) extends DynamicConsumer.Service[T] {
  override def shardedStream[R](
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
    (String, ZStream[Blocking, Throwable, Record[T]], DynamicConsumer.Checkpointer[T])
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
        subSequenceNumber = None,
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

  def make[T](refCheckpointedList: Ref[Seq[T]]): Task[Checkpointer[T]] =
    for {
      latestStaged <- Ref.make[Option[Record[T]]](None)
    } yield new DynamicConsumer.Checkpointer[T] {
      override private[client] def peek: UIO[Option[Record[T]]] = latestStaged.get

      override def stage(r: Record[T]): UIO[Unit] = latestStaged.set(Some(r))

      override def checkpoint: ZIO[Blocking, Throwable, Unit] =
        latestStaged.get.flatMap {
          case Some(record) =>
            refCheckpointedList.update { seq =>
              val x: Seq[T]    = seq
              val y: Record[T] = record
              seq :+ record.data
            } *>
              latestStaged.update {
                case Some(r) if r == record => None
                case r                      => r // A newer record may have been staged by now
              }
          case None         => UIO.unit
        }
    }
}

object DynamicConsumerFake {

  /**
   * A constructor for a fake shard, for use with the `DynamicConsumer.fake` ZLayer function. It takes a list of `List[T]` and produces
   * a ZStream of fake shards from it.
   * @param serializer A `Serializer` used to convert elements to the ByteBuffer type expected by `DynamicConsumer`
   * @param lists list of shards - each shard is represented by a List of `T`
   * @tparam R Environment for `Serializer`
   * @tparam T Type of the list element
   * @return A ZStream of fake shard with a generated shard name of the form `shardN`, where `N` is a zero based index
   * @see `DynamicConsumer.fake`
   */
  def shardsFromIterables[R, T](
    serializer: Serializer[R, T],
    lists: List[T]*
  ): ZStream[Any, Nothing, (String, ZStream[R, Throwable, ByteBuffer])] = {
    val listOfShards = lists.zipWithIndex.map {
      case (xs, i) => (s"shard$i", ZStream.fromIterable(xs).mapM(serializer.serialize))
    }
    ZStream.fromIterable(listOfShards)
  }

  /**
   * A constructor for a fake shard, for use with the `DynamicConsumer.fake` ZLayer function. It takes a list ZStream of type `T` and produces
   * a ZStream of fake shards from it.
   * @param serializer A `Serializer` used to convert elements to the ByteBuffer type expected by `DynamicConsumer`
   * @param streams list of shards - each shard is represented by a ZStream of `T`
   * @tparam R Environment for `Serializer`
   * @tparam T Type of the ZStream element
   * @return A ZStream of fake shard with a generated shard name of the form `shardN`, where `N` is a zero based index
   * @see `DynamicConsumer.fake`
   */
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
