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
import zio.logging.log
import zio.stream.ZStream.{ fromEffect, unfoldM }

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import nl.vroste.zio.kinesis.client.Util
import zio.logging.Logging

object PollingFetcher {
  import Consumer.retryOnThrottledWithSchedule

  def make(
    streamDescription: StreamDescription,
    config: FetchMode.Polling,
    emitDiagnostic: DiagnosticEvent => UIO[Unit]
  ): ZManaged[Clock with Client with Logging, Throwable, Fetcher] =
    for {
      // Max 5 calls per second (globally)
      getShardIterator <- throttledFunction(5, 1.second)((Client.getShardIterator _).tupled)
      env              <- ZIO.environment[Client with Clock with Logging].toManaged_
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

        } yield retryStream(
          ZStream
            .repeatEffectWith(pollWithDelay, Schedule.fixed(config.interval))
            .catchAll {
              case None    => ZStream.empty
              case Some(e) =>
                ZStream.unwrap(
                  log.warn(s"Error in PollingFetcher for shard ${shardId}, will retry. ${e}").as(ZStream.fail(e))
                )
            }
            .flattenChunks,
          config.throttlingBackoff
        ).provide(env)
      }.provide(env)
    }

  def retryStream[R, R1 <: R, E, O](stream: ZStream[R, E, O], schedule: Schedule[R1, E, _]): ZStream[R1, E, O] =
    ZStream.unwrap {
      for {
        s0    <- schedule.initial
        state <- Ref.make[schedule.State](s0)
      } yield {
        def streamWithRetry: ZStream[R1, E, O] =
          stream
            .catchAll(e =>
              ZStream.unwrap {
                (for {
                  s        <- state.get
                  newState <- schedule.update(e, s)
                } yield newState).fold(
                  _ => ZStream.fail(e),
                  newState =>
                    ZStream.fromEffect(state.set(newState)) *> streamWithRetry.tap(_ =>
                      schedule.initial.flatMap(state.set)
                    )
                )
              }
            )

        streamWithRetry
      }
    }
}
