package nl.vroste.zio.kinesis.client

import java.nio.ByteBuffer

import nl.vroste.zio.kinesis.client.DynamicConsumer.{ Checkpointer, Record }
import nl.vroste.zio.kinesis.client.serde.{ Deserializer, Serializer }
import software.amazon.awssdk.services.kinesis.model.EncryptionType
import software.amazon.kinesis.common.InitialPositionInStreamExtended
import zio._
import zio.blocking.Blocking
import zio.stream.ZStream

private[client] class DynamicConsumerFake(
  shards: ZStream[Any, Nothing, (String, ZStream[Any, Throwable, ByteBuffer])],
  refCheckpointedList: Ref[Seq[Any]]
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
    def record(shardName: String, recData: T): Record[T] =
      new Record[T](
        sequenceNumber = "sequenceNumber",
        approximateArrivalTimestamp = java.time.Instant.now(),
        data = recData,
        partitionKey = "partitionKey",
        encryptionType = EncryptionType.NONE,
        subSequenceNumber = 0L,
        explicitHashKey = "explicitHashKey",
        aggregated = false,
        shardId = shardName
      )

    val value1: ZStream[R with Blocking, Throwable, (String, ZStream[Any, Throwable, Record[T]], Checkpointer)] =
      shards.flatMap {
        case (shardName, stream) =>
          ZStream.fromEffect {
            val value: ZIO[R with Blocking, Throwable, (String, ZStream[Any, Throwable, Record[T]], Checkpointer)] =
              for {
                env   <- ZIO.environment[R with Blocking]
                tuple <- CheckpointerFake.make(refCheckpointedList).map { checkpointer =>
                           (
                             shardName,
                             stream.mapM(deserializer.deserialize(_).map(record(shardName, _)).provide(env)),
                             checkpointer
                           )
                         }
              } yield tuple
            value
          }
      }
    value1

  }
}

object CheckpointerFake {
  def make(refCheckpointedList: Ref[Seq[Any]]): Task[Checkpointer] =
    for {
      latestStaged <- Ref.make[Option[Record[_]]](None)
    } yield new DynamicConsumer.Checkpointer {
      override private[client] def peek: UIO[Option[Record[_]]] = latestStaged.get

      override def stage(r: Record[_]): UIO[Unit] = latestStaged.set(Some(r))

      override def checkpoint: ZIO[Blocking, Throwable, Unit] =
        latestStaged.get.flatMap {
          case Some(record) =>
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
  type Shard = (String, ZStream[Any, Nothing, ByteBuffer])
  def makeShard[R, T](
    serializer: Serializer[R, T],
    name: String,
    list: List[T]
  ): UIO[(String, ZStream[R, Throwable, ByteBuffer])] = {
    println("")
    val stream: ZStream[R, Throwable, ByteBuffer]               = for {
      s <- ZStream.fromIterable(list).mapM(serializer.serialize)
    } yield s
    val value: UIO[(String, ZStream[R, Throwable, ByteBuffer])] = UIO((name, stream))
    value
  }
}
