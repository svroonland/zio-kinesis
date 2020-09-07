package nl.vroste.zio.kinesis.client

import java.nio.ByteBuffer

import nl.vroste.zio.kinesis.client.DynamicConsumer.{ Checkpointer, Record }
import nl.vroste.zio.kinesis.client.serde.{ Deserializer, Serializer }
import software.amazon.awssdk.services.kinesis.model.EncryptionType
import software.amazon.kinesis.common.InitialPositionInStreamExtended
import zio._
import zio.blocking.Blocking
import zio.stream.ZStream

private[client] class DynamicConsumerFake2(
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
        case (s, stream) =>
          ZStream.fromEffect {
            val value: ZIO[R with Blocking, Throwable, (String, ZStream[Any, Throwable, Record[T]], Checkpointer)] =
              for {
                env <- ZIO.environment[R with Blocking]
                t   <- FakeCheckpointer2.make(refCheckpointedList).map { checkpointer =>
                       (s, stream.mapM(bb => deserializer.deserialize(bb).map(record(s, _)).provide(env)), checkpointer)
                     }
              } yield t
            value
          }
      }
    value1

  }
}

object FakeCheckpointer2 {
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

/*
TODO end to end solution - make an outer stream with a single shard and test that it runs
 */

object DynamicConsumerFake2 {
  type Shard = (String, ZStream[Any, Nothing, ByteBuffer])
  def makeShard[R, T](
    serializer: Serializer[R, T],
    name: String,
    list: List[T]
  ): UIO[(String, ZStream[R, Throwable, ByteBuffer])] = { // TODO: do we have to provide R inside this function?
    println("")
//    val x: ZIO[Any, Nothing, ByteBuffer]                        = ZIO.unit.as(ByteBuffer.wrap(Array(Byte.MinValue)))
//    val xx: UIO[List[ByteBuffer]]                               = ???
//    val stream: ZStream[Any, Nothing, ByteBuffer]               = ZStream.fromIterableM(xx)
    val s1: ZStream[R, Throwable, ByteBuffer]                   = for {
      s <- ZStream.fromIterable(list).mapM(serializer.serialize(_))
    } yield s
// ZStream.fromIterable(list).map(serializer.serialize(_))
//    for {
//
//    } yield ???
    val value: UIO[(String, ZStream[R, Throwable, ByteBuffer])] = UIO((name, s1))
    value
  }
}
