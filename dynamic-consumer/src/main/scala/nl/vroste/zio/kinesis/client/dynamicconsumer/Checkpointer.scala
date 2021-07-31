package nl.vroste.zio.kinesis.client.dynamicconsumer

import nl.vroste.zio.kinesis.client.dynamicconsumer.DynamicConsumer.{ Checkpointer, CheckpointerInternal, Record }
import software.amazon.kinesis.processor.RecordProcessorCheckpointer
import zio.{ Ref, Task, UIO, ZIO }
import zio.blocking.Blocking
import zio.logging.Logger

private[dynamicconsumer] trait CheckpointerInternal extends Checkpointer {
  def setMaxSequenceNumber(lastSequenceNumber: String): UIO[Unit]
  def markEndOfShard: UIO[Unit]
  def checkEndOfShardCheckpointRequired: Task[Unit]
}

private[dynamicconsumer] object Checkpointer {
  def make(
    kclCheckpointer: RecordProcessorCheckpointer,
    logger: Logger[String]
  ): UIO[CheckpointerInternal] =
    for {
      latestStaged                 <- Ref.make[Option[Record[_]]](None)
      lastCheckpointed             <- Ref.make[Option[String]](None)
      maxSequenceNumber            <- Ref.make[Option[String]](None)
      endOfShardCheckpointRequired <- Ref.make[Boolean](false)
    } yield new Checkpointer with CheckpointerInternal {
      override def stage(r: Record[_]): UIO[Unit] =
        latestStaged.set(Some(r))

      override def checkpoint: ZIO[Blocking, Throwable, Unit] =
        latestStaged.get.flatMap {
          case Some(record) =>
            for {
              _ <- logger.trace(s"about to checkpoint: shardId=${record.shardId} partitionKey=${record.partitionKey}")
              _ <- zio.blocking.blocking {
                     Task(kclCheckpointer.checkpoint(record.sequenceNumber, record.subSequenceNumber.getOrElse(0L)))
                   }
              _ <- latestStaged.update {
                     case Some(r) if r == record => None
                     case r                      => r // A newer record may have been staged by now
                   }
              // TODO both seq and subseq
              _ <- lastCheckpointed.set(Some(record.sequenceNumber))
              // TODO both seq and subseq
              _ <- endOfShardCheckpointRequired
                     .set(false)
                     .whenM(maxSequenceNumber.get.map(_.contains(record.sequenceNumber)))
            } yield ()
          case None         => UIO.unit
        }

      override private[client] def peek: UIO[Option[Record[_]]] = latestStaged.get

      override def setMaxSequenceNumber(lastSequenceNumber: String): UIO[Unit] =
        maxSequenceNumber.set(Some(lastSequenceNumber))

      override def markEndOfShard: UIO[Unit] =
        for {
          lastCheckpointed <- lastCheckpointed.get
          maxSeqNr         <- maxSequenceNumber.get
          _                <- (maxSeqNr, lastCheckpointed) match {
                 case (None, _)                              => ZIO.unit
                 case (Some(max), Some(last)) if max == last => ZIO.unit
                 case (Some(_), _)                           => endOfShardCheckpointRequired.set(true)
               }
        } yield ()

      override def checkEndOfShardCheckpointRequired: Task[Unit] =
        ZIO
          .fail(
            new IllegalStateException("Record at end of shard must be checkpointed before checkpointer shutdown")
          )
          .tapError(e => UIO(println(e)))
          .whenM(endOfShardCheckpointRequired.get)
    }
}
