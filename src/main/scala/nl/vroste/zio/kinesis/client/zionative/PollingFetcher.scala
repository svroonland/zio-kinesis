package nl.vroste.zio.kinesis.client.zionative
import nl.vroste.zio.kinesis.client.AdminClient.StreamDescription
import nl.vroste.zio.kinesis.client.Client
import nl.vroste.zio.kinesis.client.Client.ShardIteratorType
import software.amazon.awssdk.services.kinesis.model.Shard
import zio.{ Chunk, Ref, ZIO, ZManaged }
import zio.clock.Clock
import zio.stream.ZStream
import zio.duration._
import scala.jdk.CollectionConverters._
import nl.vroste.zio.kinesis.client.zionative.Fetcher
import nl.vroste.zio.kinesis.client.Util.throttledFunction

object PollingFetcher {
  import Consumer.retryOnThrottledWithSchedule

  def make(
    client: Client,
    streamDescription: StreamDescription,
    config: FetchMode.Polling
  ): ZManaged[Clock, Throwable, Fetcher] =
    for {
      // Max 5 calls per second (globally)
      getShardIterator <- throttledFunction(5, 1.second)((client.getShardIterator _).tupled)
    } yield Fetcher { (shard, startingPosition) =>
      ZStream.unwrapManaged {
        for {
          // GetRecords can be called up to 5 times per second per shard
          getRecordsThrottled  <- throttledFunction(5, 1.second)((client.getRecords _).tupled)
          initialShardIterator <-
            getShardIterator((streamDescription.streamName, shard.shardId(), startingPosition)).toManaged_
          shardIterator        <- Ref.make[String](initialShardIterator).toManaged_

          pollWithDelay <- makePollWithDelayIfNoResult(config.delay) {
                             for {
                               currentIterator <- shardIterator.get
                               response        <- getRecordsThrottled((currentIterator, config.batchSize))
                                             .retry(retryOnThrottledWithSchedule(config.backoff))
                                             .asSomeError
                               records          = response.records.asScala.toList
                               _                = println(s"${shard.shardId()}: Got ${records.size} records")
                               _               <- Option(response.nextShardIterator).map(shardIterator.set).getOrElse(ZIO.fail(None))
                             } yield Chunk.fromIterable(records.map(Consumer.toConsumerRecord(_, shard.shardId())))
                           }.toManaged_
        } yield ZStream.repeatEffectChunkOption(pollWithDelay)
      }
    }

  /**
   * Creates an effect that executes `poll` but with preceded by a delay if the previous call to `poll`
   * returned an empty chunk
   */
  def makePollWithDelayIfNoResult[R, E, A](delay: Duration)(
    poll: ZIO[R, E, Chunk[A]]
  ): ZIO[Any, Nothing, ZIO[Clock with R, E, Chunk[A]]] =
    Ref.make[Boolean](false).map { delayRef =>
      ZIO.sleep(delay).whenM(delayRef.get) *> poll.tap(r => delayRef.set(r.isEmpty))
    }
}
