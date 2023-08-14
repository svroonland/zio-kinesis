package nl.vroste.zio.kinesis.client.zionative

import nl.vroste.zio.kinesis.client.{ Record, Util }
import zio.stream.{ ZSink, ZStream }
import zio._

/**
 * Error indicating that while checkpointing it was discovered that the lease for a shard was stolen
 */
case object ShardLeaseLost

private[zionative] trait CheckpointerInternal {
  def setMaxSequenceNumber(lastSequenceNumber: ExtendedSequenceNumber): UIO[Unit]
  def markEndOfShard(): UIO[Unit]
}

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
   * @param r
   *   Record to checkpoint
   * @return
   *   Effect that completes immediately
   */
  def stage(r: Record[_]): UIO[Unit]

  /**
   * Helper method that ensures that a checkpoint is staged when 'effect' completes successfully, even when the fiber is
   * interrupted. When 'effect' fails or is itself interrupted, the checkpoint is not staged.
   *
   * @param effect
   *   Effect to execute
   * @param r
   *   Record to stage a checkpoint for
   * @return
   *   Effect that completes with the result of 'effect'
   */
  def stageOnSuccess[R, E, A](effect: ZIO[R, E, A])(r: Record[_]): ZIO[R, E, A] =
    effect.onExit {
      case Exit.Success(_) => stage(r)
      case _               => ZIO.unit
    }

  /**
   * Checkpoint the last staged checkpoint
   *
   * Checkpointing has 'up to and including' semantics, meaning that after a restart, the worker will continue from the
   * record after the last checkpointed record.
   *
   * Checkpointing may fail when another worker has taken over the lease. It is recommended that users catch this error
   * and recover the stream with a `ZStream.empty` to stop processing the shard.
   *
   * Checkpointing may also fail due to transient connection/service issues. The retrySchedule determines if and when to
   * retry.
   *
   * @param retrySchedule
   *   When checkpointing fails with a Throwable, retry according to this schedule. This helps to be robust against
   *   transient connection/service failures. The schedule receives the Throwable as input, which can be used to ignore
   *   certain exceptions. The default value is an infinite exponential backoff between 1 second and 1 minute. Note that
   *   ShardLeaseLost is not handled by this retry schedule.
   */
  def checkpoint[R](
    retrySchedule: Schedule[R, Throwable, Any] = Util.exponentialBackoff(1.second, 1.minute, maxRecurs = Some(5))
  ): ZIO[R, Either[Throwable, ShardLeaseLost.type], Unit]

  private[client] def checkpointAndRelease: ZIO[Any, Either[Throwable, ShardLeaseLost.type], Unit]

  /**
   * Immediately checkpoint this record
   *
   * For performance benefits it is recommended to batch checkpoints
   *
   * @param retrySchedule
   *   When checkpointing fails with a Throwable, retry according to this schedule. This helps to be robust against
   *   transient connection/service failures. The schedule receives the Throwable as input, which can be used to ignore
   *   certain exceptions. The default value is an infinite exponential backoff between 1 second and 1 minute. Note that
   *   ShardLeaseLost is not handled by this retry schedule.
   */
  def checkpointNow[R](
    r: Record[_],
    retrySchedule: Schedule[R, Throwable, Any] = Util.exponentialBackoff(1.second, 1.minute, maxRecurs = Some(5))
  ): ZIO[R, Either[Throwable, ShardLeaseLost.type], Unit] =
    stage(r) *> checkpoint[R](retrySchedule)

  /**
   * Helper method to add batch checkpointing to a shard stream
   *
   * Usage: shardStream.viaFunction(checkpointer.checkpointBatched(1000, 1.second))
   *
   * @param nr
   *   Maximum number of records before checkpointing
   * @param interval
   *   Maximum interval before checkpointing
   * @param retrySchedule
   *   Schedule to apply for retrying when checkpointing fails with an exception
   * @return
   *   Function that results in a ZStream that produces Unit values for successful checkpoints, fails with an exception
   *   when the retry schedule is exhausted or becomes an empty stream when the lease for this shard is lost, thereby
   *   ending the stream.
   */
  def checkpointBatched[R](
    nr: Long,
    interval: Duration,
    retrySchedule: Schedule[Any, Throwable, Any] = Util.exponentialBackoff(1.second, 1.minute, maxRecurs = Some(5))
  ): ZStream[R, Throwable, Any] => ZStream[R, Throwable, Unit] =
    _.aggregateAsyncWithin(ZSink.foldUntil[Any, Unit]((), nr)((_, _) => ()), Schedule.fixed(interval))
      .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
      .tap(_ => checkpoint(retrySchedule))
      .catchAll {
        case Left(e)               =>
          ZStream.fail(e)
        case Right(ShardLeaseLost) =>
          ZStream.empty

      }

  /**
   * Get the last made checkpoint for this shard
   */
  def lastCheckpoint: UIO[Option[ExtendedSequenceNumber]]
}
