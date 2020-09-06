package nl.vroste.zio.kinesis.client.zionative
import io.github.vigoo.zioaws.kinesis.model.Shard
import nl.vroste.zio.kinesis.client.zionative.LeaseCoordinator.AcquiredLease
import zio.clock.Clock
import zio.logging.Logging
import zio.random.Random
import zio.stream.ZStream
import zio.{ Promise, UIO, ZIO }

private[zionative] trait LeaseCoordinator {
  def makeCheckpointer(shardId: String): ZIO[Clock with Logging, Throwable, Checkpointer with CheckpointerInternal]

  def getCheckpointForShard(shardId: String): UIO[Option[Either[SpecialCheckpoint, ExtendedSequenceNumber]]]

  def acquiredLeases: ZStream[Clock, Throwable, AcquiredLease]

  def updateShards(shards: Map[String, Shard.ReadOnly]): UIO[Unit]

  def childShardsDetected(childShards: Seq[Shard.ReadOnly]): ZIO[Clock with Logging with Random, Throwable, Unit]
}

private[zionative] object LeaseCoordinator {
  final case class AcquiredLease(shardId: String, leaseLost: Promise[Nothing, Unit])
}
