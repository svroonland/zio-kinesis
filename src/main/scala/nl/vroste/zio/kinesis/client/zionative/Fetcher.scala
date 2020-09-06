package nl.vroste.zio.kinesis.client.zionative

import io.github.vigoo.zioaws.kinesis.model.{ Record, Shard, StartingPosition }
import nl.vroste.zio.kinesis.client.zionative.Fetcher.EndOfShard
import zio.clock.Clock
import zio.stream.ZStream

// TODO make all private stuff private
private[zionative] trait Fetcher {

  /**
   * Stream of records on the shard with the given ID, starting from the startingPosition
   *
   * May complete when the shard ends
   *
   * Can fail with a Throwable or an end-of-shard indicator
   */
  def shardRecordStream(
    shardId: String,
    startingPosition: StartingPosition
  ): ZStream[Clock, Either[Throwable, EndOfShard], Record]
}

private[zionative] object Fetcher {
  case class EndOfShard(childShards: Seq[Shard.ReadOnly])

  def apply(
    f: (
      String,
      StartingPosition
    ) => ZStream[Clock, Either[Throwable, EndOfShard], Record]
  ): Fetcher =
    (shard, startingPosition) => f(shard, startingPosition)
}
