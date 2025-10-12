package nl.vroste.zio.kinesis.client.zionative.fetcher
import nl.vroste.zio.kinesis.client.StreamIdentifier
import nl.vroste.zio.kinesis.client.Util._
import nl.vroste.zio.kinesis.client.zionative.Consumer.{ childShardToShard, retryOnThrottledWithSchedule }
import nl.vroste.zio.kinesis.client.zionative.Fetcher.EndOfShard
import nl.vroste.zio.kinesis.client.zionative.{ DiagnosticEvent, FetchMode, Fetcher }
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException
import zio._
import zio.aws.core.AwsError
import zio.aws.kinesis.Kinesis
import zio.aws.kinesis.model._
import zio.aws.kinesis.model.primitives.{
  GetRecordsInputLimit,
  MillisBehindLatest,
  SequenceNumber,
  ShardIterator,
  StreamARN
}
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
  private case class PollState(shardIteratorType: ShardIteratorType, sequenceNumber: Option[SequenceNumber])

  def make(
    streamIdentifier: StreamIdentifier,
    config: FetchMode.Polling,
    emitDiagnostic: DiagnosticEvent => UIO[Unit]
  ): ZIO[Scope with Kinesis, Throwable, Fetcher] =
    for {
      env              <- ZIO.environment[Kinesis]
      getShardIterator <- throttledFunction(getShardIteratorRateLimit, 1.second)(Kinesis.getShardIterator)
    } yield Fetcher { (shardId, startingPosition: StartingPosition) =>
      val initialize: ZIO[
        Scope,
        Nothing,
        (Ref[PollState], GetRecordsRequest => ZIO[Kinesis, AwsError, GetRecordsResponse.ReadOnly])
      ] = for {
        _                   <- ZIO.logInfo(s"Creating PollingFetcher for shard ${shardId} with starting position ${startingPosition}")
        pollState           <- Ref.make(PollState(startingPosition.`type`, startingPosition.sequenceNumber.toOption))
        getRecordsThrottled <- throttledFunction(getRecordsRateLimit, 1.second)(Kinesis.getRecords)
      } yield (pollState, getRecordsThrottled)

      def streamFromSequenceNumber(
        iteratorType: ShardIteratorType,
        sequenceNr: Option[SequenceNumber],
        getRecordsThrottled: GetRecordsRequest => ZIO[Kinesis, AwsError, GetRecordsResponse.ReadOnly]
      ): ZStream[Kinesis, Throwable, GetRecordsResponse.ReadOnly] = ZStream.unwrapScoped {
        for {
          initialShardIterator <-
            getShardIterator(
              GetShardIteratorRequest(
                shardId = shardId,
                shardIteratorType = iteratorType,
                startingSequenceNumber = sequenceNr,
                streamName = streamIdentifier.name,
                streamARN = streamIdentifier.arn
              )
            )
              .map(_.shardIterator.toOption.get)
              .mapError(_.toThrowable)
              .retry(retryOnThrottledWithSchedule(config.throttlingBackoff))
          doPoll               <-
            makePollEffectWithShardIterator(initialShardIterator, doPoll(_, getRecordsThrottled, streamIdentifier.arn))
        } yield ZStream
          .repeatZIOWithSchedule(doPoll, config.pollSchedule)
          .catchAll {
            case None    =>
              ZStream.empty
            case Some(e) =>
              ZStream.fail(e)
          }
      }

      def makePollEffectWithShardIterator[R](
        initialShardIterator: ShardIterator,
        poll: ShardIterator => ZIO[R, Option[Throwable], GetRecordsResponse.ReadOnly]
      ): ZIO[Scope, Nothing, ZIO[R, Option[Throwable], GetRecordsResponse.ReadOnly]] =
        Ref
          .make(Option(initialShardIterator))
          .map { shardIterator =>
            for {
              currentIterator <- shardIterator.get
              currentIterator <- ZIO.fromOption(currentIterator)
              response        <- poll(currentIterator)
              _               <- shardIterator.set(response.nextShardIterator.toOption)
            } yield response
          }

      // Failure with None indicates that there's no next shard iterator and the shard has ended
      def doPoll(
        currentIterator: ShardIterator,
        getRecordsThrottled: GetRecordsRequest => ZIO[Kinesis, AwsError, GetRecordsResponse.ReadOnly],
        streamArn: Option[StreamARN]
      ): ZIO[Kinesis, Option[Throwable], GetRecordsResponse.ReadOnly] =
        for {
          responseWithDuration <-
            getRecordsThrottled(
              GetRecordsRequest(currentIterator, Some(GetRecordsInputLimit(config.batchSize)), streamArn)
            )
              .mapError(_.toThrowable)
              .tapError(e => ZIO.logWarning(s"Error GetRecords for shard ${shardId}: ${e}"))
              .retry(retryOnThrottledWithSchedule(config.throttlingBackoff))
              .asSomeError
              .timed
          (duration, response)  = responseWithDuration
          millisBehindLatest   <-
            ZIO.fromOption(response.millisBehindLatest.toOption).orElse(ZIO.succeed(MillisBehindLatest(0)))
          _                    <- emitDiagnostic(
                                    DiagnosticEvent.PollComplete(shardId, response.records.size, millisBehindLatest.millis, duration)
                                  )
        } yield response

      def detectEndOfShard =
        (_: ZStream[Kinesis, Throwable, GetRecordsResponse.ReadOnly])
          .mapError(Left(_): Either[Throwable, EndOfShard])
          .flatMap { response =>
            if (response.childShards.toList.flatten.nonEmpty && response.nextShardIterator.isEmpty)
              ZStream.succeed(response) ++ (ZStream.fromZIO(
                ZIO.logDebug(s"PollingFetcher found end of shard for ${shardId}")
              ) *>
                ZStream.fail(Right(EndOfShard(response.childShards.toList.flatten.map(childShardToShard)))))
            else
              ZStream.succeed(response)
          }

      ZStream.unwrapScoped {
        initialize.map { case (pollState, getRecordsThrottled) =>
          val streamFromLastSequenceNr =
            ZStream.fromZIO(pollState.get).flatMap { case PollState(iteratorType, sequenceNr) =>
              streamFromSequenceNumber(iteratorType, sequenceNr, getRecordsThrottled)
                .tap(response =>
                  ZIO.foreachDiscard(response.records.lastOption.map(_.sequenceNumber))(s =>
                    pollState.set(PollState(ShardIteratorType.AFTER_SEQUENCE_NUMBER, Some(s)))
                  )
                )
            }

          streamFromLastSequenceNr.catchSome { case _: ExpiredIteratorException =>
            ZStream.fromZIO(ZIO.logInfo("Iterator expired. Refreshing")) *> streamFromLastSequenceNr
          }
            .onError(e => ZIO.logWarning(s"Error in PollingFetcher for shard ${shardId}: ${e}"))
            .retry(config.throttlingBackoff)
        }
      }
        .viaFunction(detectEndOfShard)
        .buffer(config.bufferNrBatches)
        .mapConcat(_.records)
        .ensuring(ZIO.logDebug(s"PollingFetcher for shard ${shardId} closed"))
        .provideEnvironment(env)
    }

  private val getShardIteratorRateLimit = 5
  private val getRecordsRateLimit       = 5
}
