package nl.vroste.zio.kinesis.client.native
import nl.vroste.zio.kinesis.client.AdminClient.StreamDescription
import nl.vroste.zio.kinesis.client.Client
import nl.vroste.zio.kinesis.client.Client.ShardIteratorType
import software.amazon.awssdk.services.kinesis.model.Shard
import zio.{ Chunk, Ref, ZIO, ZManaged }
import zio.clock.Clock
import zio.stream.ZStream
import zio.duration._
import scala.jdk.CollectionConverters._
import nl.vroste.zio.kinesis.client.native.Fetcher

object PollingFetcher {
  import Consumer.retryOnThrottledWithSchedule

  def make(
    client: Client,
    streamDescription: StreamDescription,
    config: FetchMode.Polling
  ): ZManaged[Clock, Throwable, Fetcher] =
    for {
      // Max 5 calls per second (globally)
      getShardIterator <-
        Util.throttledFunction(5, 1.second, (client.getShardIterator _).tupled).map(Function.untupled(_))
    } yield Fetcher { (shard, startingPosition) =>
      ZStream.unwrapManaged {
        for {
          // GetRecords can be called up to 5 times per second per shard
          getRecordsThrottled  <-
            Util.throttledFunction(5, 1.second, Function.tupled(client.getRecords _)).map(Function.untupled(_))
          initialShardIterator <-
            getShardIterator(streamDescription.streamName, shard.shardId(), startingPosition).toManaged_
          delayRef             <- Ref.make[Boolean](false).toManaged_
          shardIterator        <- Ref.make[String](initialShardIterator).toManaged_
        } yield ZStream.repeatEffectChunkOption {
          for {
            _               <- (
                     ZIO.sleep(config.delay)
                 ).whenM(delayRef.get)
            currentIterator <- shardIterator.get
            response        <- getRecordsThrottled(currentIterator, config.batchSize)
                          .retry(retryOnThrottledWithSchedule(config.backoff))
                          .asSomeError
            records          = response.records.asScala.toList
            _                = println(s"${shard.shardId()}: Got ${records.size} records")
            _               <- delayRef.set(records.isEmpty)
            _               <- Option(response.nextShardIterator).map(shardIterator.set).getOrElse(ZIO.fail(None))
          } yield Chunk.fromIterable(records.map(Consumer.toConsumerRecord(_, shard.shardId())))
        }
      }
    }
}
