//package nl.vroste.zio.kinesis.client
//
//import nl.vroste.zio.kinesis.client.DynamicConsumer.{ Checkpointer, Record }
//import nl.vroste.zio.kinesis.client.serde.{ Deserializer, Serializer }
//import software.amazon.awssdk.services.kinesis.model.EncryptionType
//import software.amazon.kinesis.common.InitialPositionInStreamExtended
//import zio._
//import zio.blocking.Blocking
//import zio.stream.ZStream
//
//private[client] class DynamicConsumerFake[T2](
//  shards: ZStream[Any, Nothing, (String, ZStream[Any, Nothing, T2])],
//  serializer: Serializer[R, T2],
//  refCheckpointedList: Ref[Seq[T2]]
//) extends DynamicConsumer.Service {
//  override def shardedStream[R, T <: T2](
//    streamName: String,
//    applicationName: String,
//    deserializer: Deserializer[R, T],
//    requestShutdown: UIO[Unit],
//    initialPosition: InitialPositionInStreamExtended,
//    isEnhancedFanOut: Boolean,
//    leaseTableName: Option[String],
//    workerIdentifier: String,
//    maxShardBufferSize: Int
//  ): ZStream[
//    Blocking with R,
//    Throwable,
//    (String, ZStream[Blocking, Throwable, Record[T]], DynamicConsumer.Checkpointer)
//  ] = {
//    def record(shardName: String, recData: T): Record[T] =
//      new Record[T](
//        sequenceNumber = "sequenceNumber",
//        approximateArrivalTimestamp = java.time.Instant.now(),
//        data = recData,
//        partitionKey = "partitionKey",
//        encryptionType = EncryptionType.NONE,
//        subSequenceNumber = 0L,
//        explicitHashKey = "explicitHashKey",
//        aggregated = false,
//        shardId = shardName
//      )
//
////    ZStream.fromEffect(FakeCheckpointer.make(refCheckpointedList)).flatMap { checkpointer =>
////      shards.map {
////        case (s, stream) => (s, stream.map(record(s, _)), checkpointer)
////      }
////    }
//
////    def as[T](r: Record[_]): Option[Record[T]] =
////      r match {
////        case r: Record[T] => Some(r)
////        case _            => None
////      }
//
//    def foo(r: T2): ZIO[R, Throwable, T] =
//      for {
////        _ <- ZIO.accessM[Blocking](_.get.blocking(ZIO.unit))
//        b <- serializer.serialize(r)
//        t <- deserializer.deserialize(b)
//      } yield t
//
//    shards.flatMap {
//      case (s, stream) =>
//        val x: ZStream[R, Throwable, T] = stream.mapM(r => foo(r))
//        ZStream.fromEffect(FakeCheckpointer.make(refCheckpointedList)).map { checkpointer =>
//          (s, stream.mapM(r => foo(r).map(record(s, _))), checkpointer)
//        }
//        ZStream.fromEffect {
//          for {
//            env <- ZIO.environment[R with Blocking]
//            t   <- FakeCheckpointer.make(refCheckpointedList).map { checkpointer =>
//                   (s, stream.mapM(r => foo(r).map(record(s, _)).provide(env)), checkpointer)
//                 }
//          } yield t
//        }
//    }
//
//  }
//}
//
//object FakeCheckpointer {
//  def make[T](refCheckpointedList: Ref[Seq[T]]): UIO[Checkpointer] =
//    for {
//      latestStaged <- Ref.make[Option[Record[_]]](None)
//    } yield new DynamicConsumer.Checkpointer {
//      override private[client] def peek: UIO[Option[Record[_]]] = latestStaged.get
//
//      override def stage(r: Record[_]): UIO[Unit] = latestStaged.set(Some(r))
//
//      override def checkpoint: ZIO[Blocking, Throwable, Unit] =
//        latestStaged.modify {
//          case Some(record: T) => (refCheckpointedList.update(_ :+ record), None)
//        }
//    }
//}
