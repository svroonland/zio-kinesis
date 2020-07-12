package nl.vroste.zio.kinesis.client.zionative.fetcher
import nl.vroste.zio.kinesis.client.zionative.{ Consumer, DiagnosticEvent, FetchMode, Fetcher }
import nl.vroste.zio.kinesis.client.{ Client, Util }
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException
import zio._
import zio.clock.Clock
import zio.duration._
import zio.logging.{ log, Logging }
import zio.stream.ZStream

import scala.jdk.CollectionConverters._

object PollingFetcher {
  import Consumer.retryOnThrottledWithSchedule
  import Util._

  def make(
    streamName: String,
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
          initialShardIterator <- getShardIterator((streamName, shardId, startingPosition))
                                    .retry(retryOnThrottledWithSchedule(config.throttlingBackoff))
                                    .toManaged_
          shardIterator        <- Ref.make[Option[String]](Some(initialShardIterator)).toManaged_

          // Failure with None indicates that there's no next shard iterator and the shard has ended
          doPoll                          = for {
                     currentIterator      <- shardIterator.get
                     currentIterator      <- ZIO.fromOption(currentIterator)
                     responseWithDuration <-
                       getRecordsThrottled(
                         (currentIterator, config.batchSize)
                       ).retry(retryOnThrottledWithSchedule(config.throttlingBackoff))
                         .asSomeError
                         .retry(
                           Schedule.fixed(100.millis) && Schedule.recurs(3)
                         ) // There is a race condition in kinesalite, see https://github.com/mhart/kinesalite/issues/25
                         .timed
                     (duration, response)  = responseWithDuration
                     _                    <- shardIterator.set(Option(response.nextShardIterator))
                     _                    <- emitDiagnostic(
                            DiagnosticEvent.PollComplete(
                              shardId,
                              response.records.size,
                              response.millisBehindLatest().toLong.millis,
                              duration
                            )
                          )
                   } yield response

          doPollAndRetryOnExpiredIterator = doPoll.catchSome {
                                              case Some(e: ExpiredIteratorException) =>
                                                log.warn(s"Shard iterator expired: ${e}") *>
                                                  getShardIterator(
                                                    (streamName, shardId, startingPosition)
                                                  ).retry(retryOnThrottledWithSchedule(config.throttlingBackoff))
                                                    .asSomeError
                                                    .flatMap(it => shardIterator.set(Some(it))) *> doPoll
                                            }

        } yield repeatEffectWith(doPollAndRetryOnExpiredIterator, config.pollSchedule).catchAll {
          case None    =>
            ZStream.empty
          case Some(e) =>
            ZStream.unwrap(
              log.warn(s"Error in PollingFetcher for shard ${shardId}: ${e}").as(ZStream.fail(e))
            )
        }.mapConcatChunk(response => Chunk.fromIterable(response.records.asScala))
          .retry(
            config.throttlingBackoff.tapOutput {
              case (delay, retryNr) =>
                log.info(s"PollingFetcher will make make retry attempt nr ${retryNr} in ${delay.toMillis} millis")
            }
          )
      }.provide(env)
    }

  // Like ZStream.repeatEffectWith but does not delay the outputs
  // TODO Wait for https://github.com/zio/zio/pull/3959 for this to be merged
  def repeatEffectWith[R, E, A](effect: ZIO[R, E, A], schedule: Schedule[R, A, _]): ZStream[R, E, A] =
    ZStream.fromEffect(schedule.initial zip effect).flatMap {
      case (initialScheduleState, value) =>
        ZStream.succeed(value) ++ ZStream.unfoldM((initialScheduleState, value)) {
          case (state, lastValue) =>
            schedule
              .update(lastValue, state)
              .foldM(_ => ZIO.succeed(Option.empty), newState => effect.map(value => Some((value, (newState, value)))))
        }
    }
}
