package nl.vroste.zio.kinesis.client.zionative
import nl.vroste.zio.kinesis.client.Client.ShardIteratorType
import software.amazon.awssdk.services.kinesis.model.{ ChildShard, Record => KinesisRecord }
import zio.clock.Clock
import zio.stream.ZStream

// TODO make all private stuff private
private[zionative] trait Fetcher {

  /**
   * Stream of records on the shard with the given ID, starting from the startingPosition
   *
   * May complete when the shard ends
   */
  def shardRecordStream(
    shardId: String,
    startingPosition: ShardIteratorType
  ): ZStream[Clock, Either[Throwable, Seq[ChildShard]], KinesisRecord]
}

private[zionative] object Fetcher {
  def apply(
    f: (String, ShardIteratorType) => ZStream[Clock, Either[Throwable, Seq[ChildShard]], KinesisRecord]
  ): Fetcher =
    (shard, startingPosition) => f(shard, startingPosition)
}
