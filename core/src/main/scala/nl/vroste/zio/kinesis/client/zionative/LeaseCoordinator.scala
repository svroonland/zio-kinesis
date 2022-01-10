package nl.vroste.zio.kinesis.client.zionative
import zio.aws.kinesis.model.Shard
import nl.vroste.zio.kinesis.client.zionative.LeaseCoordinator.AcquiredLease
import zio.aws.kinesis.model.primitives.ShardId
import zio.stream.ZStream
import zio.{ Promise, UIO, ZIO }
import zio.{ Clock, Random }

private[zionative] trait LeaseCoordinator {
  def makeCheckpointer(
    shardId: String
  ): ZIO[Clock with Random, Throwable, Checkpointer with CheckpointerInternal]

  def getCheckpointForShard(shardId: String): UIO[Option[Either[SpecialCheckpoint, ExtendedSequenceNumber]]]

  def acquiredLeases: ZStream[Clock, Throwable, AcquiredLease]

  def updateShards(shards: Map[ShardId, Shard.ReadOnly]): UIO[Unit]

  def childShardsDetected(
    childShards: Seq[Shard.ReadOnly]
  ): ZIO[Clock with Random, Throwable, Unit]
}

private[zionative] object LeaseCoordinator {
  final case class AcquiredLease(shardId: String, leaseLost: Promise[Nothing, Unit])
}
