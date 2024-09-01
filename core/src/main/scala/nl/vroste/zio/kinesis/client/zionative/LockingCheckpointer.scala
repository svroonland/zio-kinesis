package nl.vroste.zio.kinesis.client.zionative

import nl.vroste.zio.kinesis.client.Record
import zio._

final class LockingCheckpointer[T] private (
  stateRef: Ref[LockingCheckpointer.State],
  checkpointer: Checkpointer,
  maxCheckpointSize: Long,
  maxCheckpointInterval: Duration
) {
  import LockingCheckpointer.State

  /**
   * Lock the checkpointer on the key of the record. If the key is already locked, the caller will be blocked until the
   * key is unlocked by checkpointing.
   */
  def lock(record: Record[T]): UIO[Unit] = {
    val key = record.partitionKey

    stateRef.modify { state =>
      val blocked = state.locked.contains(key)
      val next    = if (blocked) state.copy(blockedOn = Some(key)) else state.addLocked(key)
      ((blocked, next), next)
    }.flatMap {
      case (true, state) =>
        // The key we are blocked on is already staged, so we can checkpoint immediately
        val checkpointNeeded = state.staged.contains(key)
        state.requestCheckpoint.when(checkpointNeeded) *> state.checkpointed.await
      case (false, _)    => ZIO.unit
    }
  }

  /**
   * Stage a record for checkpointing. If the checkpointer is locked on the key, a checkpoint will be performed
   * immediately.
   */
  def stage(record: Record[T]): UIO[Unit] = {
    val key = record.partitionKey

    checkpointer.stage(record) *>
      stateRef
        .updateAndGet(_.stage(record.partitionKey))
        .tap { state =>
          val checkpointNeeded = state.blockedOn.contains(key) || state.staged.size >= maxCheckpointSize
          state.requestCheckpoint.when(checkpointNeeded)
        }
        .unit
  }

  /**
   * Checkpoint all checkpoints that are staged and unlock the keys. Retry failed checkpoints according to the provided
   * schedule. This method will loop forever and should therefore be run in a separate fiber.
   */
  def checkpointLoop[R](
    checkpointRetrySchedule: Schedule[R, Throwable, Any]
  ): ZIO[R, Either[Throwable, ShardLeaseLost.type], Nothing] =
    stateRef.get
      .flatMap(_.checkpointRequested.await)
      .timeout(maxCheckpointInterval)
      .tap(_ => checkpointer.checkpoint(checkpointRetrySchedule) *> unlock)
      .forever

  private def unlock: UIO[Unit] =
    for {
      nextCheckpointRequested <- Promise.make[Nothing, Unit]
      nextCheckpointed        <- Promise.make[Nothing, Unit]
      _                       <- stateRef.modify { case State(locked, staged, blockedOn, _, checkpointed) =>
                                   val nextState = State(
                                     locked -- staged,
                                     Set.empty,
                                     blockedOn.filterNot(staged.contains),
                                     nextCheckpointRequested,
                                     nextCheckpointed
                                   )
                                   (checkpointed.succeed(()), nextState)
                                 }.flatten
    } yield ()
}

object LockingCheckpointer {
  private final case class State(
    locked: Set[String],
    staged: Set[String],
    blockedOn: Option[String],
    checkpointRequested: Promise[Nothing, Unit],
    checkpointed: Promise[Nothing, Unit]
  ) {

    def addLocked(key: String): State = copy(locked = locked + key)

    def stage(key: String): State = copy(staged = staged + key)

    def requestCheckpoint: UIO[Unit] = checkpointRequested.succeed(()).unit
  }

  def make[T](
    checkpointer: Checkpointer,
    maxCheckpointSize: Long,
    maxCheckpointInterval: Duration
  ): UIO[LockingCheckpointer[T]] = for {
    checkpointRequested <- Promise.make[Nothing, Unit]
    checkpointed        <- Promise.make[Nothing, Unit]
    initialState         = State(Set.empty, Set.empty, None, checkpointRequested, checkpointed)
    stateRef            <- Ref.make[State](initialState)
  } yield new LockingCheckpointer(stateRef, checkpointer, maxCheckpointSize, maxCheckpointInterval)
}
