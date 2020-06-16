package nl.vroste.zio.kinesis.client.zionative

import zio.duration.Duration

/**
 * Events for diagnostics of Kinesis streaming
 **/
sealed trait DiagnosticEvent

object DiagnosticEvent {
  case class PollComplete(shardId: String, nrRecords: Int, behindLatest: Duration, duration: Duration)
      extends DiagnosticEvent
  case class ShardLeaseLost(shardId: String)                                            extends DiagnosticEvent
  case class LeaseAcquired(shardId: String, checkpoint: Option[ExtendedSequenceNumber]) extends DiagnosticEvent
  case class LeaseRenewed(shardId: String)                                              extends DiagnosticEvent
  case class LeaseReleased(shardId: String)                                             extends DiagnosticEvent
  case class LeaseStolen(shardId: String, previousOwner: String)                        extends DiagnosticEvent
  case class Checkpoint(shardId: String, checkpoint: ExtendedSequenceNumber)            extends DiagnosticEvent
}
