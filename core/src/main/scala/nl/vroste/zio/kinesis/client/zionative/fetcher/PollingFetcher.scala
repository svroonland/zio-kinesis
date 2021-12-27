package nl.vroste.zio.kinesis.client.zionative.fetcher
import io.github.vigoo.zioaws.kinesis
import io.github.vigoo.zioaws.kinesis.Kinesis
import io.github.vigoo.zioaws.kinesis.model._
import nl.vroste.zio.kinesis.client.zionative.Consumer.childShardToShard
import nl.vroste.zio.kinesis.client.zionative.Fetcher.EndOfShard
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
 *   - GetRecords can retrieve up to 10 MB of data per call from a single shard, and up to 10,000 records per call. Each
 *     call to GetRecords is counted as one read transaction.
 *   - Each shard can support up to five read transactions per second. Each read transaction can provide up to 10,000
 *     records with an upper quota of 10 MB per transaction.
 *   - Each shard can support up to a maximum total data read rate of 2 MB per second via GetRecords. i If a call to
 *     GetRecords returns 10 MB, subsequent calls made within the next 5 seconds throw an exception.
 *   - GetShardIterator: max 5 calls per second globally
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
      getShardIterator <- throttledFunction(getShardIteratorRateLimit, 1.second)(kinesis.getShardIterator)
    } yield Fetcher { (shardId, startingPosition: StartingPosition) =>
      ZStream.unwrapManaged {
        for {
          _                    <- log
                                    .info(s"Creating PollingFetcher for shard ${shardId} with starting position ${startingPosition}")
                                    .toManaged_
          initialShardIterator <-
            getShardIterator(
              GetShardIteratorRequest(streamName, shardId, startingPosition.`type`, startingPosition.sequenceNumber)
            ).map(_.shardIteratorValue.get)
              .mapError(_.toThrowable)
              .retry(retryOnThrottledWithSchedule(config.throttlingBackoff))
              .mapError(Left(_): Either[Throwable, EndOfShard])
              .toManaged_
          getRecordsThrottled  <- throttledFunction(getRecordsRateLimit, 1.second)(kinesis.getRecords)
          shardIterator        <- Ref.make[Option[String]](Some(initialShardIterator)).toManaged_

          // Failure with None indicates that there's no next shard iterator and the shard has ended
          doPoll = for {
                     currentIterator      <- shardIterator.get
                     currentIterator      <- ZIO.fromOption(currentIterator)
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
                         .retry(retryOnThrottledWithSchedule(config.throttlingBackoff))
                         .retry(
                           Schedule.fixed(100.millis) && Schedule.recurs(3)
                         ) // There is a race condition in kinesalite, see https://github.com/mhart/kinesalite/issues/25
                         .asSomeError
                         .timed
                     (duration, response)  = responseWithDuration
                     _                    <- shardIterator.set(response.nextShardIteratorValue)
                     millisBehindLatest   <- response.millisBehindLatest.mapError(e => Some(e.toThrowable))
                     _                    <- emitDiagnostic(
                                               DiagnosticEvent.PollComplete(
                                                 shardId,
                                                 response.recordsValue.size,
                                                 millisBehindLatest.millis,
                                                 duration
                                               )
                                             )
                   } yield response

          shardStream = ZStream
                          .repeatEffectWith(doPoll, config.pollSchedule)
                          .catchAll {
                            case None    => // TODO do we still need the None in combination with nextShardIterator?
                              ZStream.empty
                            case Some(e) =>
                              ZStream.fromEffect(
                                log.warn(s"Error in PollingFetcher for shard ${shardId}: ${e}")
                              ) *> ZStream.fail(e)
                          }
                          .retry(config.throttlingBackoff)
                          .buffer(config.bufferNrBatches)
                          .mapError(Left(_): Either[Throwable, EndOfShard])
                          .flatMap { response =>
                            if (
                              response.childShardsValue.toList.flatten.nonEmpty && response.nextShardIteratorValue.isEmpty
                            )
                              ZStream.succeed(response) ++ (ZStream.fromEffect(
                                log.debug(s"PollingFetcher found end of shard for ${shardId}")
                              ) *>
                                ZStream.fail(
                                  Right(
                                    EndOfShard(response.childShardsValue.toList.flatten.map(childShardToShard))
                                  )
                                ))
                            else
                              ZStream.succeed(response)
                          }
                          .mapConcat(_.recordsValue)
        } yield shardStream
      }.ensuring(log.debug(s"PollingFetcher for shard ${shardId} closed"))
        .provide(env)

    }

  private val getShardIteratorRateLimit = 5
  private val getRecordsRateLimit       = 5
}
