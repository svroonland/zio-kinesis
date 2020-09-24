package nl.vroste.zio.kinesis.client.zionative.leasecoordinator
import nl.vroste.zio.kinesis.client.Record
import nl.vroste.zio.kinesis.client.zionative._
import nl.vroste.zio.kinesis.client.zionative.leasecoordinator.DefaultCheckpointer.{ State, UpdateCheckpoint }
import zio.clock.Clock
import zio.logging.{ log, Logging }
import zio._

private[zionative] class DefaultCheckpointer(
  shardId: String,
  env: Logging,
  state: Ref[State],
  permit: Semaphore,
  updateCheckpoint: UpdateCheckpoint,
  releaseLease: ZIO[Any, Throwable, Unit]
) extends Checkpointer
    with CheckpointerInternal {
  def checkpoint[R](
    retrySchedule: Schedule[Clock with R, Throwable, Any]
  ): ZIO[Clock with R, Either[Throwable, ShardLeaseLost.type], Unit] =
    doCheckpoint(false)
      .retry(retrySchedule +++ Schedule.stop) // Only retry Left[Throwable]

  override def checkpointAndRelease: ZIO[Any, Either[Throwable, ShardLeaseLost.type], Unit] =
    state.get.flatMap {
      case State(_, lastCheckpoint, maxSeqNr, shardEnded) =>
        log
          .debug(
            s"Checkpoint and release for shard ${shardId}, maxSeqNr=${maxSeqNr}, " +
              s"lastCheckpoint=${lastCheckpoint}, shardEnded=${shardEnded}"
          )
          .provide(env)
    } *> doCheckpoint(release = true)

  private def doCheckpoint(release: Boolean): ZIO[Any, Either[Throwable, ShardLeaseLost.type], Unit] =
    /*
     * This uses a semaphore to ensure that two concurrent calls to `checkpoint` do not attempt
     * to process the last staged record
     *
   * It is fine to stage new records for checkpointing though, those will be processed in the
     * next call tho `checkpoint`. Staging is therefore not protected by a semaphore.
     *
   * If a new record is staged while the checkpoint operation is busy, the next call to checkpoint
     * will use that record's sequence number.
     *
   * If checkpointing fails, the last staged sequence number is left unchanged, even it has already been
     * updated.
     */
    permit.withPermit {
      for {
        s                <- state.get
        lastStaged        = s.staged
        maxSequenceNumber = s.maxSequenceNumber
        endOfShardSeen    = s.shardEnded
        lastCheckpoint    = s.lastCheckpoint
        _                <- lastStaged match {
               case Some(sequenceNr) =>
                 val checkpoint: Either[SpecialCheckpoint, ExtendedSequenceNumber] =
                   maxSequenceNumber
                     .filter(_ == sequenceNr && endOfShardSeen)
                     .map(_ => Left(SpecialCheckpoint.ShardEnd))
                     .getOrElse(Right(sequenceNr))

                 (updateCheckpoint(checkpoint, release) *>
                   state.update(_.copy(lastCheckpoint = checkpoint.toOption)) *>
                   // only update when the staged record has not changed while checkpointing
                   state.updateSome {
                     case s @ State(Some(staged), _, _, _) if staged == sequenceNr => s.copy(staged = None)
                   }).uninterruptible
               // Either the last checkpoint is the current max checkpoint (i.e. an empty poll at shard end)
               // or we only get that empty poll (when having resumed the shard after the last sequence nr)
               case None
                   if release && endOfShardSeen && ((maxSequenceNumber.isEmpty && lastCheckpoint.isEmpty) || lastCheckpoint
                     .exists(maxSequenceNumber.contains)) =>
                 val checkpoint = Left(SpecialCheckpoint.ShardEnd)

                 updateCheckpoint(checkpoint, release)

               case None if release  =>
                 releaseLease.mapError(Left(_))
               case None             =>
                 ZIO.unit
             }
      } yield ()
    }

  override def stage(r: Record[_]): zio.UIO[Unit] =
    state.update(_.copy(staged = Some(ExtendedSequenceNumber(r.sequenceNumber, r.subSequenceNumber.getOrElse(0L)))))

  override def setMaxSequenceNumber(lastSequenceNumber: ExtendedSequenceNumber): UIO[Unit] =
    state.update(_.copy(maxSequenceNumber = Some(lastSequenceNumber)))

  override def markEndOfShard(): UIO[Unit] =
    state.update(_.copy(shardEnded = true))
}

private[zionative] object DefaultCheckpointer {
  type UpdateCheckpoint = (
    Either[SpecialCheckpoint, ExtendedSequenceNumber],
    Boolean // release
  ) => ZIO[Any, Either[Throwable, ShardLeaseLost.type], Unit]

  case class State(
    staged: Option[ExtendedSequenceNumber],
    lastCheckpoint: Option[ExtendedSequenceNumber],
    maxSequenceNumber: Option[ExtendedSequenceNumber],
    shardEnded: Boolean
  )

  object State {
    val empty = State(None, None, None, false)
  }
}
