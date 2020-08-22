package nl.vroste.zio.kinesis.client.zionative
import io.github.vigoo.zioaws.kinesis.model.{ Record, StartingPosition }
import zio.clock.Clock
import zio.stream.ZStream

// TODO make all private stuff private
private[zionative] trait Fetcher {

  /**
   * Stream of records on the shard with the given ID, starting from the startingPosition
   *
   * May complete when the shard ends
   */
  def shardRecordStream(shardId: String, startingPosition: StartingPosition): ZStream[Clock, Throwable, Record]
}

private[zionative] object Fetcher {
  def apply(f: (String, StartingPosition) => ZStream[Clock, Throwable, Record]): Fetcher =
    (shard, startingPosition) => f(shard, startingPosition)
}
