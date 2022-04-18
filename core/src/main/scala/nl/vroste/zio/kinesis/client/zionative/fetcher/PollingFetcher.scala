package nl.vroste.zio.kinesis.client.zionative.fetcher

import nl.vroste.zio.kinesis.client.Util
import nl.vroste.zio.kinesis.client.zionative.Consumer.childShardToShard
import nl.vroste.zio.kinesis.client.zionative.Fetcher.EndOfShard
import nl.vroste.zio.kinesis.client.zionative.{ Consumer, DiagnosticEvent, FetchMode, Fetcher }
import zio._
import zio.aws.kinesis.Kinesis
import zio.aws.kinesis.model._
import zio.aws.kinesis.model.primitives.{ GetRecordsInputLimit, MillisBehindLatest, ShardIterator, StreamName }
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
    streamName: StreamName,
    config: FetchMode.Polling,
    emitDiagnostic: DiagnosticEvent => UIO[Unit]
  ): ZIO[Scope with Kinesis, Throwable, Fetcher] =
    for {
      env              <- ZIO.environment[Kinesis]
      getShardIterator <- throttledFunction(getShardIteratorRateLimit, 1.second)(Kinesis.getShardIterator)
    } yield Fetcher { (shardId, startingPosition: StartingPosition) =>
      ZStream.unwrapScoped {
        for {
          _ <- ZIO
                 .logInfo(s"Creating PollingFetcher for shard ${shardId} with starting position ${startingPosition}")

          initialShardIterator <-
            getShardIterator(
              GetShardIteratorRequest(streamName, shardId, startingPosition.`type`, startingPosition.sequenceNumber)
            ).map(_.shardIterator.toOption.get)
              .mapError(_.toThrowable)
              .retry(retryOnThrottledWithSchedule(config.throttlingBackoff))
              .mapError(Left(_): Either[Throwable, EndOfShard])

          getRecordsThrottled <- throttledFunction(getRecordsRateLimit, 1.second)(Kinesis.getRecords)
          shardIterator       <- Ref.make[Option[ShardIterator]](Some(initialShardIterator))

          // Failure with None indicates that there's no next shard iterator and the shard has ended
          doPoll = for {
                     currentIterator      <- shardIterator.get
                     currentIterator      <- ZIO.fromOption(currentIterator)
                     responseWithDuration <- getRecordsThrottled(
                                               GetRecordsRequest(
                                                 currentIterator,
                                                 Some(GetRecordsInputLimit(config.batchSize))
                                               )
                                             )
                                               .mapError(_.toThrowable)
                                               .retry(retryOnThrottledWithSchedule(config.throttlingBackoff))
                                               .asSomeError
                                               .timed
                     (duration, response)  = responseWithDuration
                     _                    <- shardIterator.set(response.nextShardIterator.toOption)
                     millisBehindLatest   <-
                       ZIO.fromOption(response.millisBehindLatest.toOption).orElse(ZIO.succeed(MillisBehindLatest(0)))
                     _                    <- emitDiagnostic(
                                               DiagnosticEvent.PollComplete(
                                                 shardId,
                                                 response.records.size,
                                                 millisBehindLatest.millis,
                                                 duration
                                               )
                                             )
                   } yield response

          shardStream = ZStream
                          .repeatZIOWithSchedule(doPoll, config.pollSchedule)
                          .catchAll {
                            case None    => // TODO do we still need the None in combination with nextShardIterator?
                              ZStream.empty
                            case Some(e) =>
                              ZStream.fromZIO(
                                ZIO.logWarning(s"Error in PollingFetcher for shard ${shardId}: ${e}")
                              ) *> ZStream.fail(e)
                          }
                          .retry(config.throttlingBackoff)
//                          .buffer(config.bufferNrBatches) // TODO see https://github.com/zio/zio/issues/6649
                          .mapError(Left(_): Either[Throwable, EndOfShard])
                          .flatMap { response =>
                            if (response.childShards.toList.flatten.nonEmpty && response.nextShardIterator.isEmpty)
                              ZStream.succeed(response) ++ (ZStream.fromZIO(
                                ZIO.logDebug(s"PollingFetcher found end of shard for ${shardId}")
                              ) *>
                                ZStream.fail(
                                  Right(
                                    EndOfShard(response.childShards.toList.flatten.map(childShardToShard))
                                  )
                                ))
                            else
                              ZStream.succeed(response)
                          }
                          .mapConcat(_.records)
        } yield shardStream
      }.ensuring(ZIO.logDebug(s"PollingFetcher for shard ${shardId} closed"))
        .provideEnvironment(env)

    }

  private val getShardIteratorRateLimit = 5
  private val getRecordsRateLimit       = 5
}
