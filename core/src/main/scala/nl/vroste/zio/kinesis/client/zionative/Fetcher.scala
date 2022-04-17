package nl.vroste.zio.kinesis.client.zionative

import nl.vroste.zio.kinesis.client.zionative.Fetcher.EndOfShard
import zio.aws.kinesis.model.{ primitives, Record, Shard, StartingPosition }
import zio.stream.ZStream

private[zionative] trait Fetcher {

  /**
   * Stream of records on the shard with the given ID, starting from the startingPosition
   *
   * May complete when the shard ends
   *
   * Can fail with a Throwable or an end-of-shard indicator
   */
  def shardRecordStream(
    shardId: primitives.ShardId,
    startingPosition: StartingPosition
  ): ZStream[Any, Either[Throwable, EndOfShard], Record.ReadOnly]
}

private[zionative] object Fetcher {
  case class EndOfShard(childShards: Seq[Shard.ReadOnly])

  def apply(
    f: (
      primitives.ShardId,
      StartingPosition
    ) => ZStream[Any, Either[Throwable, EndOfShard], Record.ReadOnly]
  ): Fetcher =
    (shard, startingPosition) => f(shard, startingPosition)
}
