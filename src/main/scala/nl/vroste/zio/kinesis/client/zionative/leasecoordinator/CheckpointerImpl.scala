package nl.vroste.zio.kinesis.client.zionative.leasecoordinator
import nl.vroste.zio.kinesis.client.Record
import nl.vroste.zio.kinesis.client.zionative._
import zio.clock.Clock
import zio.logging.{ log, Logging }
import zio.random.Random
import zio._

sealed trait CheckpointAction

private[zionative] class CheckpointerImpl(
  shardId: String,
  env: Clock with Logging with Random,
  maxSequenceNumber: Ref[Option[ExtendedSequenceNumber]],
  lastCheckpoint: Ref[Option[ExtendedSequenceNumber]],
  shardEnded: Ref[Boolean],
  permit: Semaphore,
  staged: Ref[Option[ExtendedSequenceNumber]],
  updateCheckpoint: (
    Either[SpecialCheckpoint, ExtendedSequenceNumber],
    Boolean, // release
    Boolean  // shard ended
  ) => ZIO[Any, Either[Throwable, ShardLeaseLost.type], Unit],
  releaseLease: ZIO[Any, Throwable, Unit]
) extends Checkpointer
    with CheckpointerInternal {
  def checkpoint[R](
    retrySchedule: Schedule[Clock with R, Throwable, Any]
  ): ZIO[Clock with R, Either[Throwable, ShardLeaseLost.type], Unit] =
    doCheckpoint(false)
      .retry(retrySchedule +++ Schedule.stop) // Only retry Left[Throwable]

  override def checkpointAndRelease: ZIO[Any, Either[Throwable, ShardLeaseLost.type], Unit] =
    (maxSequenceNumber.get zip lastCheckpoint.get zip shardEnded.get).flatMap {
      case ((maxSeqNr, lastCheckpoint), shardEnded) =>
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
     *
   * TODO make a test for that: staging while checkpointing should not lose the staged checkpoint.
     */
    permit.withPermit {
      for {
        lastStaged          <- staged.get
        maxSequenceNumber   <- maxSequenceNumber.get
        endOfShardSeen      <- shardEnded.get
        lastCheckpointValue <- lastCheckpoint.get
        _                   <- lastStaged match {
               case Some(sequenceNr) =>
                 val checkpoint: Either[SpecialCheckpoint, ExtendedSequenceNumber] =
                   maxSequenceNumber
                     .filter(_ == sequenceNr && endOfShardSeen)
                     .map(_ => Left(SpecialCheckpoint.ShardEnd))
                     .getOrElse(Right(sequenceNr))

                 (updateCheckpoint(checkpoint, release, checkpoint.isLeft) *>
                   lastCheckpoint.set(checkpoint.toOption) *>
                   // only update when the staged record has not changed while checkpointing
                   staged.updateSome { case Some(s) if s == sequenceNr => None }).uninterruptible
               // Either the last checkpoint is the current max checkpoint (i.e. an empty poll at shard end)
               // or we only get that empty poll (when having resumed the shard after the last sequence nr)
               case None
                   if release && endOfShardSeen && ((maxSequenceNumber.isEmpty && lastCheckpointValue.isEmpty) || lastCheckpointValue
                     .exists(maxSequenceNumber.contains)) =>
                 val checkpoint = Left(SpecialCheckpoint.ShardEnd)

                 updateCheckpoint(checkpoint, release, true)

               case None if release  =>
                 releaseLease.mapError(Left(_))
               case None             =>
                 ZIO.unit
             }
      } yield ()
    }

  override def stage(r: Record[_]): zio.UIO[Unit] =
    staged.set(Some(ExtendedSequenceNumber(r.sequenceNumber, r.subSequenceNumber.getOrElse(0L))))

  override def setMaxSequenceNumber(lastSequenceNumber: ExtendedSequenceNumber): UIO[Unit] =
    maxSequenceNumber.set(Some(lastSequenceNumber))

  override def markEndOfShard(): UIO[Unit] =
    shardEnded.set(true)
}
