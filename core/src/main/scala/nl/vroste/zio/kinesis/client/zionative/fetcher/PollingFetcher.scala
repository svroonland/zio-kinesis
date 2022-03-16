package nl.vroste.zio.kinesis.client.zionative.fetcher
import io.github.vigoo.zioaws.core.AwsError
import io.github.vigoo.zioaws.kinesis
import io.github.vigoo.zioaws.kinesis.Kinesis
import io.github.vigoo.zioaws.kinesis.model._
import io.github.vigoo.zioaws.kinesis.model.primitives.{ SequenceNumber, ShardIterator }
import nl.vroste.zio.kinesis.client.Util
import nl.vroste.zio.kinesis.client.Util._
import nl.vroste.zio.kinesis.client.zionative.Consumer.{ childShardToShard, retryOnThrottledWithSchedule }
import nl.vroste.zio.kinesis.client.zionative.Fetcher.EndOfShard
import nl.vroste.zio.kinesis.client.zionative.{ Consumer, DiagnosticEvent, FetchMode, Fetcher }
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException
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
  def make(
    streamName: String,
    config: FetchMode.Polling,
    emitDiagnostic: DiagnosticEvent => UIO[Unit]
  ): ZManaged[Clock with Kinesis with Logging, Throwable, Fetcher] =
    for {
      env              <- ZIO.environment[Kinesis with Clock with Logging].toManaged_
      getShardIterator <- throttledFunction(getShardIteratorRateLimit, 1.second)(kinesis.getShardIterator)
    } yield Fetcher { (shardId, startingPosition: StartingPosition) =>
      val initialize: ZManaged[
        Clock with Logging,
        Nothing,
        (Ref[Option[SequenceNumber]], GetRecordsRequest => ZIO[Kinesis, AwsError, GetRecordsResponse.ReadOnly])
      ] = for {
        _                   <- log
                                 .info(s"Creating PollingFetcher for shard ${shardId} with starting position ${startingPosition}")
                                 .toManaged_
        sequenceNumber      <- Ref.make(startingPosition.sequenceNumber).toManaged_
        getRecordsThrottled <- throttledFunction(getRecordsRateLimit, 1.second)(kinesis.getRecords)
      } yield (sequenceNumber, getRecordsThrottled)

      def streamFromSequenceNumber(
        sequenceNr: Option[SequenceNumber],
        getRecordsThrottled: GetRecordsRequest => ZIO[Kinesis, AwsError, GetRecordsResponse.ReadOnly]
      ): ZStream[Clock with Logging with Kinesis, Throwable, GetRecordsResponse.ReadOnly] = ZStream.unwrapManaged {
        for {
          initialShardIterator <-
            getShardIterator(GetShardIteratorRequest(streamName, shardId, startingPosition.`type`, sequenceNr))
              .map(_.shardIteratorValue.get)
              .mapError(_.toThrowable)
              .retry(retryOnThrottledWithSchedule(config.throttlingBackoff))
              .toManaged_
          doPoll               <- makePollEffectWithShardIterator(initialShardIterator, doPoll(_, getRecordsThrottled))
        } yield ZStream
          .repeatEffectWith(doPoll, config.pollSchedule)
          .catchAll {
            case None    =>
              ZStream.empty
            case Some(e) =>
              ZStream.fromEffect(
                log.warn(s"Error in PollingFetcher for shard ${shardId}: ${e}")
              ) *> ZStream.fail(e)
          }
      }

      def makePollEffectWithShardIterator[R](
        initialShardIterator: ShardIterator,
        poll: ShardIterator => ZIO[R, Option[Throwable], GetRecordsResponse.ReadOnly]
      ): ZManaged[Any, Nothing, ZIO[R, Option[Throwable], GetRecordsResponse.ReadOnly]] =
        Ref
          .make(Option(initialShardIterator))
          .toManaged_
          .map { shardIterator =>
            for {
              currentIterator <- shardIterator.get
              currentIterator <- ZIO.fromOption(currentIterator)
              response        <- poll(currentIterator)
              _               <- shardIterator.set(response.nextShardIteratorValue)
            } yield response
          }

      // Failure with None indicates that there's no next shard iterator and the shard has ended
      def doPoll(
        currentIterator: ShardIterator,
        getRecordsThrottled: GetRecordsRequest => ZIO[Kinesis, AwsError, GetRecordsResponse.ReadOnly]
      ): ZIO[Clock with Logging with Kinesis, Option[Throwable], GetRecordsResponse.ReadOnly] =
        for {
          responseWithDuration <- getRecordsThrottled(GetRecordsRequest(currentIterator, Some(config.batchSize)))
                                    .mapError(_.toThrowable)
                                    .tapError(e => log.warn(s"Error GetRecords for shard ${shardId}: ${e}"))
                                    .retry(retryOnThrottledWithSchedule(config.throttlingBackoff))
                                    .asSomeError
                                    .timed
          (duration, response)  = responseWithDuration
          millisBehindLatest   <- response.millisBehindLatest.mapError(e => Some(e.toThrowable))
          _                    <- emitDiagnostic(
                                    DiagnosticEvent.PollComplete(shardId, response.recordsValue.size, millisBehindLatest.millis, duration)
                                  )
        } yield response

      def detectEndOfShard =
        (_: ZStream[Kinesis with Clock with Logging, Throwable, GetRecordsResponse.ReadOnly])
          .mapError(Left(_): Either[Throwable, EndOfShard])
          .flatMap { response =>
            if (response.childShardsValue.toList.flatten.nonEmpty && response.nextShardIteratorValue.isEmpty)
              ZStream.succeed(response) ++ (ZStream.fromEffect(
                log.debug(s"PollingFetcher found end of shard for ${shardId}")
              ) *>
                ZStream.fail(Right(EndOfShard(response.childShardsValue.toList.flatten.map(childShardToShard)))))
            else
              ZStream.succeed(response)
          }

      ZStream.unwrapManaged {
        initialize.map { case (sequenceNumberRef, getRecordsThrottled) =>
          val streamFromLastSequenceNr =
            ZStream.fromEffect(sequenceNumberRef.get).flatMap { sequenceNr =>
              streamFromSequenceNumber(sequenceNr, getRecordsThrottled)
                .tap(response => sequenceNumberRef.set(response.recordsValue.lastOption.map(_.sequenceNumberValue)))
            }

          streamFromLastSequenceNr.catchSome { case _: ExpiredIteratorException =>
            ZStream.fromEffect(log.info("Iterator expired. Refreshing")) *> streamFromLastSequenceNr
          }
            .retry(config.throttlingBackoff)
        }
      }
        .via(detectEndOfShard)
        .buffer(config.bufferNrBatches)
        .mapConcat(_.recordsValue)
        .ensuring(log.debug(s"PollingFetcher for shard ${shardId} closed"))
        .provide(env)
    }

  private val getShardIteratorRateLimit = 5
  private val getRecordsRateLimit       = 5
}
