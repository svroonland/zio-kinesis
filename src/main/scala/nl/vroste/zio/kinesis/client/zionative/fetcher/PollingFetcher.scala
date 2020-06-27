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

        } yield ZStream
          .repeatEffectWith(pollWithDelay, Schedule.fixed(config.interval))
          .catchAll {
            case None    => ZStream.empty
            case Some(e) =>
              ZStream.unwrap(
                log.warn(s"Error in PollingFetcher for shard ${shardId}: ${e}").as(ZStream.fail(e))
              )
          }
          .flattenChunks
          .retry(
            config.throttlingBackoff.tapOutput {
              case (delay, retryNr) =>
                log.info(s"PollingFetcher will make make retry attempt nr ${retryNr} in ${delay.toMillis} millis")
            }
          )
          .provide(env)
      }.provide(env)
    }

  implicit class ZStreamExtensions[-R, +E, +O](val stream: ZStream[R, E, O]) extends AnyVal {

    /**
     * When the stream fails, retry it according to the given schedule
     *
     * This retries the entire stream, so will re-execute all of the stream's acquire operations.
     *
     * The schedule is reset as soon as the first element passes through the stream again.
     *
      * @param schedule Schedule receiving as input the errors of the stream
     * @return Stream outputting elements of all attempts of the stream
     */
    def retry[R1 <: R](schedule: Schedule[R1, E, _]): ZStream[R1, E, O] =
      ZStream.unwrap {
        for {
          s0    <- schedule.initial
          state <- Ref.make[schedule.State](s0)
        } yield {
          def go: ZStream[R1, E, O] =
            stream
              .catchAll(e =>
                ZStream.unwrap {
                  (for {
                    s        <- state.get
                    newState <- schedule.update(e, s)
                  } yield newState).fold(
                    _ => ZStream.fail(e), // Failure of the schedule indicates it doesn't accept the input
                    newState =>
                      ZStream.fromEffect(state.set(newState)) *> go.mapChunksM { chunk =>
                        // Reset the schedule to its initial state when a chunk is successfully pulled
                        state.set(s0).as(chunk)
                      }
                  )
                }
              )

          go
        }
      }
  }
}
