package nl.vroste.zio.kinesis.client.zionative

import zio.duration.Duration

/**
 * Events for diagnostics of Kinesis streaming
 **/
sealed trait DiagnosticEvent

object DiagnosticEvent {

  /**
   * A single round of polling of record on a shard completed with results
   *
    * @param shardId
   * @param nrRecords
   * @param behindLatest See GetRecords
   * @param duration Time the call to GetRecords took, including time needed to wait for taking into
   *   account AWS limits
   */
  case class PollComplete(shardId: String, nrRecords: Int, behindLatest: Duration, duration: Duration)
      extends DiagnosticEvent

  case class SubscribeToShard(shardId: String, behindLatest: Duration) extends DiagnosticEvent

  sealed trait LeaseEvent extends DiagnosticEvent

  /**
   * The worker acquired the lease for a shard
   *
    * @param shardId Shard ID
   * @param checkpoint The last checkpoint made for this shard
   */
  case class LeaseAcquired(shardId: String, checkpoint: Option[ExtendedSequenceNumber]) extends LeaseEvent

  /**
   * The worker discovered that it had lost the lease for the given shard
   *
   * This may be discovered during lease renewal or checkpointing
   *
    * @param shardId Shard ID
   */
  case class ShardLeaseLost(shardId: String) extends LeaseEvent

  /**
   * The worker successfully renewed the lease for the given shard
   *
    * @param shardId Shard ID
   */
  case class LeaseRenewed(shardId: String) extends LeaseEvent

  /**
   * The lease for the given shard was gracefully released
   *
    * @param shardId Shard ID
   */
  case class LeaseReleased(shardId: String) extends LeaseEvent

  /**
   * A checkpoint was made for the given shard
   *
    * @param shardId Shard ID
   * @param checkpoint Checkpoint
   */
  case class Checkpoint(shardId: String, checkpoint: ExtendedSequenceNumber) extends LeaseEvent

  sealed trait WorkerEvent extends DiagnosticEvent

  /**
   * A new worker joined after start of this worker and claimed one or more leases
   */
  case class WorkerJoined(workerId: String) extends WorkerEvent

  /**
   * A worker has left (gracefully or zombie) and has no more claimed leases
   */
  case class WorkerLeft(workerId: String) extends WorkerEvent
}
