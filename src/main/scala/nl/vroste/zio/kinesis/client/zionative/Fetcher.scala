package nl.vroste.zio.kinesis.client.zionative
import nl.vroste.zio.kinesis.client.Client.ShardIteratorType
import nl.vroste.zio.kinesis.client.zionative.Fetcher.EndOfShard
import software.amazon.awssdk.services.kinesis.model.{ Shard, Record => KinesisRecord }
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
    startingPosition: ShardIteratorType
  ): ZStream[Clock, Either[Throwable, EndOfShard], KinesisRecord]
}

private[zionative] object Fetcher {
  case class EndOfShard(childShards: Seq[Shard])

  def apply(
    f: (
      String,
      ShardIteratorType
    ) => ZStream[Clock, Either[Throwable, EndOfShard], KinesisRecord]
  ): Fetcher =
    (shard, startingPosition) => f(shard, startingPosition)
}
