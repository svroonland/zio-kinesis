package nl.vroste.zio.kinesis.client.zionative.fetcher
import nl.vroste.zio.kinesis.client.AdminClient.StreamDescription
import nl.vroste.zio.kinesis.client.Client
import nl.vroste.zio.kinesis.client.Util.throttledFunction
import nl.vroste.zio.kinesis.client.zionative.{ Consumer, DiagnosticEvent, FetchMode, Fetcher }
import software.amazon.awssdk.services.kinesis.model.Record
import zio.clock.Clock
import zio.duration._
import zio.stream.ZStream
import zio._

import scala.jdk.CollectionConverters._

object PollingFetcher {
  import Consumer.retryOnThrottledWithSchedule

  def make(
    streamDescription: StreamDescription,
    config: FetchMode.Polling,
    emitDiagnostic: DiagnosticEvent => UIO[Unit]
  ): ZManaged[Clock with Client, Throwable, Fetcher] =
    for {
      // Max 5 calls per second (globally)
      getShardIterator <- throttledFunction(5, 1.second)((Client.getShardIterator _).tupled)
      env              <- ZIO.environment[Client with Clock].toManaged_
    } yield Fetcher { (shardId, startingPosition) =>
      ZStream.unwrapManaged {
        for {
          // GetRecords can be called up to 5 times per second per shard
          getRecordsThrottled  <- throttledFunction(5, 1.second)((Client.getRecords _).tupled)
          initialShardIterator <- getShardIterator((streamDescription.streamName, shardId, startingPosition))
                                    .retry(retryOnThrottledWithSchedule(config.throttlingBackoff))
                                    .toManaged_
          shardIterator        <- Ref.make[String](initialShardIterator).toManaged_

          // Failure with None indicates that there's no next shard iterator and the shard has ended
          pollWithDelay: ZIO[Clock with Client, Option[Throwable], Chunk[Record]] =
            for {
              currentIterator      <- shardIterator.get
              responseWithDuration <-
                getRecordsThrottled((currentIterator, config.batchSize))
                  .retry(retryOnThrottledWithSchedule(config.throttlingBackoff))
                  .asSomeError
                  .retry(
                    Schedule.fixed(100.millis) && Schedule.recurs(3)
                  ) // There is a race condition in kinesalite, see https://github.com/mhart/kinesalite/issues/25
                  .timed
              (duration, response)  = responseWithDuration
              records               = response.records.asScala.toList
              _                    <- Option(response.nextShardIterator).map(shardIterator.set).getOrElse(ZIO.fail(None))
              _                    <- emitDiagnostic(
                     DiagnosticEvent.PollComplete(
                       shardId,
                       records.size,
                       response.millisBehindLatest().toLong.millis,
                       duration
                     )
                   )
            } yield Chunk.fromIterable(records)

        } yield ZStream
          .repeatEffectWith(pollWithDelay, Schedule.fixed(config.interval))
          .catchAll {
            case None    => ZStream.empty
            case Some(e) => ZStream.fail(e)
          }
          .flattenChunks
          .provide(env)
      }.provide(env)
    }
}
