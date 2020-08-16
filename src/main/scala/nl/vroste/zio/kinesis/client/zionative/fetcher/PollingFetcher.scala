package nl.vroste.zio.kinesis.client.zionative.fetcher
import io.github.vigoo.zioaws.kinesis
import io.github.vigoo.zioaws.kinesis.Kinesis
import io.github.vigoo.zioaws.kinesis.model._
import nl.vroste.zio.kinesis.client.zionative.{ Consumer, DiagnosticEvent, FetchMode, Fetcher }
import nl.vroste.zio.kinesis.client.Util
import zio._
import zio.clock.Clock
import zio.duration._
import zio.logging.{ log, Logging }
import zio.stream.ZStream

/**
 * Fetcher that uses GetRecords
 *
 * Limits (from https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html):
 * - GetRecords can retrieve up to 10 MB of data per call from a single shard, and up to 10,000 records per call.
 *   Each call to GetRecords is counted as one read transaction.
 * - Each shard can support up to five read transactions per second.
 *   Each read transaction can provide up to 10,000 records with an upper quota of 10 MB per transaction.
 * - Each shard can support up to a maximum total data read rate of 2 MB per second via GetRecords. i
 *   If a call to GetRecords returns 10 MB, subsequent calls made within the next 5 seconds throw an exception.
 * - GetShardIterator: max 5 calls per second globally
 */
object PollingFetcher {
  import Consumer.retryOnThrottledWithSchedule
  import Util._

  def make(
    streamName: String,
    config: FetchMode.Polling,
    emitDiagnostic: DiagnosticEvent => UIO[Unit]
  ): ZManaged[Clock with Kinesis with Logging, Throwable, Fetcher] =
    for {
      env              <- ZIO.environment[Kinesis with Clock with Logging].toManaged_
      getShardIterator <- throttledFunction(getShardIteratorRateLimit, 1.second)(kinesis.getShardIterator _)
    } yield Fetcher { (shardId, startingPosition: ShardIteratorType) =>
      ZStream.unwrapManaged {
        for {
          initialShardIterator <- getShardIterator(GetShardIteratorRequest(streamName, shardId, startingPosition))
                                    .map(_.shardIteratorValue.get)
                                    .mapError(_.toThrowable)
                                    .retry(retryOnThrottledWithSchedule(config.throttlingBackoff))
                                    .toManaged_
          getRecordsThrottled  <- throttledFunction(getRecordsRateLimit, 1.second)(kinesis.getRecords _)
          shardIterator        <- Ref.make[Option[String]](Some(initialShardIterator)).toManaged_

          // Failure with None indicates that there's no next shard iterator and the shard has ended
          doPoll: ZIO[Logging, Option[Throwable], GetRecordsResponse.ReadOnly] = for {
                                                                                   _                    <- log.info("Polling")
                                                                                   currentIterator      <- shardIterator.get
                                                                                   currentIterator      <-
                                                                                     ZIO.fromOption(currentIterator)
                                                                                   responseWithDuration <-
                                                                                     getRecordsThrottled(
                                                                                       GetRecordsRequest(
                                                                                         currentIterator,
                                                                                         Some(config.batchSize)
                                                                                       )
                                                                                     ).mapError(_.toThrowable)
                                                                                       .tapError(e =>
                                                                                         log.warn(
                                                                                           s"Error GetRecords for shard ${shardId}: ${e}"
                                                                                         )
                                                                                       )
                                                                                       .retry(
                                                                                         retryOnThrottledWithSchedule(
                                                                                           config.throttlingBackoff
                                                                                         )
                                                                                       )
                                                                                       .retry(
                                                                                         Schedule.fixed(
                                                                                           100.millis
                                                                                         ) && Schedule
                                                                                           .recurs(3)
                                                                                       ) // There is a race condition in kinesalite, see https://github.com/mhart/kinesalite/issues/25
                                                                                       .asSomeError
                                                                                       .timed
                                                                                   (duration, response)  =
                                                                                     responseWithDuration
                                                                                   _                    <-
                                                                                     shardIterator.set(
                                                                                       Option(
                                                                                         response.nextShardIteratorValue.get
                                                                                       )
                                                                                     )
                                                                                   millisBehindLatest   <-
                                                                                     response.millisBehindLatest
                                                                                       .mapError(
                                                                                         _.toThrowable
                                                                                       )
                                                                                   _                    <- emitDiagnostic(
                                                                                          DiagnosticEvent.PollComplete(
                                                                                            shardId,
                                                                                            response.recordsValue.size,
                                                                                            millisBehindLatest.millis,
                                                                                            duration
                                                                                          )
                                                                                        )
                                                                                 } yield response

          shardStream                                                          = ZStream
                          .repeatEffectWith(doPoll, config.pollSchedule)
                          .catchAll {
                            case None    =>
                              ZStream.empty
                            case Some(e) =>
                              ZStream.fromEffect(
                                log.warn(s"Error in PollingFetcher for shard ${shardId}: ${e}")
                              ) *> ZStream.fail(e)
                          }
                          .takeUntil(_.nextShardIterator == null)
                          .buffer(config.bufferNrBatches)
                          .mapConcatChunk(response => Chunk.fromIterable(response.recordsValue.map(_.editable)))
                          .retry(config.throttlingBackoff)
        } yield shardStream
      }.ensuring(log.debug(s"PollingFetcher for shard ${shardId} closed"))
        .provide(env)

    }

  private val getShardIteratorRateLimit = 5
  private val getRecordsRateLimit       = 5
}
