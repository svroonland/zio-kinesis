package nl.vroste.zio.kinesis.client.dynamicconsumer

import nl.vroste.zio.kinesis.client.dynamicconsumer.DynamicConsumer.{ Checkpointer, Record }
import software.amazon.kinesis.processor.RecordProcessorCheckpointer
import zio.{ Ref, Task, UIO, ZIO }
import zio.blocking.Blocking
import zio.logging.Logger

private[dynamicconsumer] trait CheckpointerInternal extends Checkpointer {
  def setMaxSequenceNumber(lastSequenceNumber: ExtendedSequenceNumber): UIO[Unit]
  def markEndOfShard: UIO[Unit]
  def checkEndOfShardCheckpointRequired: Task[Unit]
}

private[dynamicconsumer] object Checkpointer {
  def make(
    kclCheckpointer: RecordProcessorCheckpointer,
    logger: Logger[String]
  ): UIO[CheckpointerInternal] =
    for {
      latestStaged                 <- Ref.make[Option[ExtendedSequenceNumber]](None)
      lastCheckpointed             <- Ref.make[Option[ExtendedSequenceNumber]](None)
      maxSequenceNumber            <- Ref.make[Option[ExtendedSequenceNumber]](None)
      endOfShardCheckpointRequired <- Ref.make[Boolean](false)
    } yield new Checkpointer with CheckpointerInternal {
      override def stage(r: Record[_]): UIO[Unit] =
        latestStaged.set(Some(ExtendedSequenceNumber(r.sequenceNumber, r.subSequenceNumber)))

      override def checkpoint: ZIO[Blocking, Throwable, Unit] =
        latestStaged.get.flatMap {
          case Some(sequenceNumber) =>
            for {
              _ <- logger.trace(s"about to checkpoint ${sequenceNumber}")
              _ <- zio.blocking.blocking {
                     Task(
                       kclCheckpointer
                         .checkpoint(sequenceNumber.sequenceNumber, sequenceNumber.subSequenceNumber.getOrElse(0L))
                     )
                   }
              _ <- latestStaged.update {
                     case Some(r) if r == sequenceNumber => None
                     case r                              => r // A newer record may have been staged by now
                   }
              _ <- lastCheckpointed.set(Some(sequenceNumber))
              _ <- endOfShardCheckpointRequired
                     .set(false)
                     .whenM(maxSequenceNumber.get.map(_.contains(sequenceNumber)))
            } yield ()
          case None                 => UIO.unit
        }

      override private[client] def peek: UIO[Option[ExtendedSequenceNumber]] = latestStaged.get

      override def setMaxSequenceNumber(lastSequenceNumber: ExtendedSequenceNumber): UIO[Unit] =
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
