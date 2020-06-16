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
import zio.Schedule
import zio.Has
import zio.UIO

object PollingFetcher {
  import Consumer.retryOnThrottledWithSchedule

  def make(
    streamDescription: StreamDescription,
    config: FetchMode.Polling,
    emitDiagnostic: DiagnosticEvent => UIO[Unit] = _ => UIO.unit
  ): ZManaged[Clock with Has[Client], Throwable, Fetcher] =
    for {
      // Max 5 calls per second (globally)
      getShardIterator <- throttledFunction(5, 1.second)((Client.getShardIterator _).tupled)
      env              <- ZIO.environment[Has[Client] with Clock].toManaged_
    } yield Fetcher { (shard, startingPosition) =>
      ZStream.unwrapManaged {
        for {
          // GetRecords can be called up to 5 times per second per shard
          // TODO we probably also want some minimum interval here to prevent frequent polls with only a few records
          getRecordsThrottled  <- throttledFunction(5, 1.second)((Client.getRecords _).tupled)
          initialShardIterator <-
            getShardIterator((streamDescription.streamName, shard.shardId(), startingPosition)).toManaged_
          shardIterator        <- Ref.make[String](initialShardIterator).toManaged_

          pollWithDelay <- makePollWithDelayIfNoResult(config.delay) {
                             for {
                               currentIterator      <- shardIterator.get
                               responseWithDuration <-
                                 getRecordsThrottled((currentIterator, config.batchSize))
                                   .retry(retryOnThrottledWithSchedule(config.backoff))
                                   .asSomeError
                                   .retry(
                                     Schedule.fixed(100.millis) && Schedule.recurs(3)
                                   ) // There is a race condition in kinesalite, see https://github.com/mhart/kinesalite/issues/25
                                   .timed
                               (duration, response)  = responseWithDuration
                               records               = response.records.asScala.toList
                               //  _                = println(s"${shard.shardId()}: Got ${records.size} records")
                               _                    <- Option(response.nextShardIterator).map(shardIterator.set).getOrElse(ZIO.fail(None))
                               _                    <- emitDiagnostic(
                                      DiagnosticEvent.PollComplete(
                                        shard.shardId(),
                                        records.size,
                                        response.millisBehindLatest().toLong.millis,
                                        duration
                                      )
                                    )
                             } yield Chunk.fromIterable(records.map(Consumer.toConsumerRecord(_, shard.shardId())))
                           }.toManaged_
        } yield ZStream.repeatEffectChunkOption(pollWithDelay)
      }.provide(env)
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
