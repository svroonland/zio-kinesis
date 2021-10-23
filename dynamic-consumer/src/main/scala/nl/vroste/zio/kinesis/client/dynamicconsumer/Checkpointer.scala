package nl.vroste.zio.kinesis.client.dynamicconsumer

import nl.vroste.zio.kinesis.client.dynamicconsumer.DynamicConsumer.{ Checkpointer, Record }
import software.amazon.kinesis.processor.RecordProcessorCheckpointer
import zio.{ Ref, Task, UIO, ZIO }
import zio.blocking.Blocking
import zio.logging.Logger
import zio.Random

case object LastRecordMustBeCheckpointedException
    extends Exception("Record at end of shard must be checkpointed before checkpointer shutdown")

private[dynamicconsumer] trait CheckpointerInternal extends Checkpointer {
  def setMaxSequenceNumber(lastSequenceNumber: ExtendedSequenceNumber): UIO[Unit]
  def markEndOfShard: UIO[Unit]
  def checkEndOfShardCheckpointed: Task[Unit]
}

private[dynamicconsumer] object Checkpointer {
  case class State(
    latestStaged: Option[ExtendedSequenceNumber],
    lastCheckpointed: Option[ExtendedSequenceNumber],
    maxSequenceNumber: Option[ExtendedSequenceNumber],
    endOfShard: Boolean
  )

  def make(
    kclCheckpointer: RecordProcessorCheckpointer,
    logger: Logger[String]
  ): UIO[CheckpointerInternal] =
    for {
      state <- Ref.make(State(None, None, None, false))
    } yield new Checkpointer with CheckpointerInternal {
      override def stage(r: Record[_]): UIO[Unit] =
        state.update(_.copy(latestStaged = Some(ExtendedSequenceNumber(r.sequenceNumber, r.subSequenceNumber))))

      override def checkpoint: ZIO[Any, Throwable, Unit] =
        state.get.flatMap {
          case State(Some(sequenceNumber), _, _, _) =>
            for {
              _ <- logger.trace(s"about to checkpoint ${sequenceNumber}")
              _ <- zio.ZIO.blocking {
                     Task(
                       kclCheckpointer
                         .checkpoint(sequenceNumber.sequenceNumber, sequenceNumber.subSequenceNumber.getOrElse(0L))
                     )
                   }
              _ <- state.updateSome {
                     case State(Some(latestStaged), lastCheckpointed @ _, maxSequenceNumber, endOfShard) =>
                       State(
                         latestStaged = if (latestStaged == sequenceNumber) None else Some(latestStaged),
                         Some(sequenceNumber),
                         maxSequenceNumber,
                         endOfShard
                       )
                   }
            } yield ()
          case State(None, _, _, _)                 => UIO.unit
        }

      override private[client] def peek: UIO[Option[ExtendedSequenceNumber]] = state.get.map(_.latestStaged)

      override def setMaxSequenceNumber(lastSequenceNumber: ExtendedSequenceNumber): UIO[Unit] =
        state.update(_.copy(maxSequenceNumber = Some(lastSequenceNumber)))

      override def markEndOfShard: UIO[Unit] =
        state.update(_.copy(endOfShard = true))

      override def checkEndOfShardCheckpointed: Task[Unit] =
        ZIO
          .fail(LastRecordMustBeCheckpointedException)
          .whenZIO(state.get.tap(s => UIO(println(s"State at check end of shard: ${s}"))).map {
            case State(_, _, None, _)                                                  => false
            case State(_, None, Some(maxSequenceNumber @ _), endOfShard) if endOfShard => true
            case State(_, Some(lastCheckpointed), Some(maxSequenceNumber), endOfShard)
                if endOfShard && lastCheckpointed != maxSequenceNumber =>
              true
            case _                                                                     => false
          })
    }
}
