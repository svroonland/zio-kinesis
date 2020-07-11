package nl.vroste.zio.kinesis.client.zionative
import nl.vroste.zio.kinesis.client.zionative.LeaseCoordinator.AcquiredLease
import zio.clock.Clock
import zio.logging.Logging
import zio.stream.ZStream
import zio.{ Promise, UIO, ZIO }

private[zionative] trait LeaseCoordinator {
  def makeCheckpointer(shardId: String): ZIO[Clock with Logging, Throwable, Checkpointer]

  def getCheckpointForShard(shardId: String): UIO[Option[ExtendedSequenceNumber]]

  def acquiredLeases: ZStream[Clock, Throwable, AcquiredLease]
}

private[zionative] object LeaseCoordinator {
  case class AcquiredLease(shardId: String, leaseLost: Promise[Nothing, Unit])
}
