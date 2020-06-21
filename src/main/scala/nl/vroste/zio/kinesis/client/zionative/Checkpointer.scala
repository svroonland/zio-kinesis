package nl.vroste.zio.kinesis.client.zionative

import zio.ZIO
import zio.blocking.Blocking
import nl.vroste.zio.kinesis.client.DynamicConsumer.Record
import zio.UIO
import zio.Exit

/**
 * Error indicating that while checkpointing it was discovered that the lease for a shard was stolen
 */
case object ShardLeaseLost

/**
 * Staging area for checkpoints
 *
 * Guarantees that the last staged record is checkpointed upon stream shutdown / interruption
 */
trait Checkpointer {

  /**
   * Stages a record for checkpointing
   *
   * Checkpoints are not actually performed until `checkpoint` is called
   *
   * @param r Record to checkpoint
   * @return Effect that completes immediately
   */
  def stage(r: Record[_]): UIO[Unit]

  /**
   * Helper method that ensures that a checkpoint is staged when 'effect' completes
   * successfully, even when the fiber is interrupted. When 'effect' fails or is itself
   * interrupted, the checkpoint is not staged.
   *
   * @param effect Effect to execute
   * @param r Record to stage a checkpoint for
   * @return Effect that completes with the result of 'effect'
   */
  def stageOnSuccess[R, E, A](effect: ZIO[R, E, A])(r: Record[_]): ZIO[R, E, A] =
    effect.onExit {
      case Exit.Success(_) => stage(r)
      case _               => UIO.unit
    }

  /**
   * Checkpoint the last staged checkpoint
   */
  def checkpoint: ZIO[Blocking, Either[Throwable, ShardLeaseLost.type], Unit]

  private[client] def checkpointAndRelease: ZIO[Blocking, Either[Throwable, ShardLeaseLost.type], Unit]

  /**
   * Immediately checkpoint this record
   */
  def checkpointNow(r: Record[_]): ZIO[Blocking, Either[Throwable, ShardLeaseLost.type], Unit] =
    stage(r) *> checkpoint
}
