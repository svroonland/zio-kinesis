package nl.vroste.zio.kinesis.client.zionative

import nl.vroste.zio.kinesis.client.Util.ZStreamExtensions
import nl.vroste.zio.kinesis.client.serde.Deserializer
import nl.vroste.zio.kinesis.client.zionative.FetchMode.{ EnhancedFanOut, Polling }
import nl.vroste.zio.kinesis.client.zionative.Fetcher.EndOfShard
import nl.vroste.zio.kinesis.client.zionative.LeaseCoordinator.AcquiredLease
import nl.vroste.zio.kinesis.client.zionative.fetcher.{ EnhancedFanOutFetcher, PollingFetcher }
import nl.vroste.zio.kinesis.client.zionative.leasecoordinator.{ DefaultLeaseCoordinator, LeaseCoordinationSettings }
import nl.vroste.zio.kinesis.client.zionative.leaserepository.DynamoDbLeaseRepository
import nl.vroste.zio.kinesis.client.{ HttpClientBuilder, Record, Util, _ }
import software.amazon.awssdk.services.kinesis.model.{
  KmsThrottlingException,
  LimitExceededException,
  ProvisionedThroughputExceededException
}
import zio._
import zio.aws.cloudwatch.CloudWatch
import zio.aws.kinesis.Kinesis
import zio.aws.kinesis.model._
import zio.aws.kinesis.model.primitives.{ SequenceNumber, ShardId, StreamName, Timestamp }
import zio.stream.ZStream

import java.time.Instant
import scala.jdk.CollectionConverters._

final case class ExtendedSequenceNumber(sequenceNumber: String, subSequenceNumber: Long)

sealed trait FetchMode
object FetchMode {

  /**
   * Fetches records in a polling manner
   *
   * @param batchSize
   *   The maximum number of records to retrieve in one call to GetRecords. Note that Kinesis defines limits in terms of
   *   the maximum size in bytes of this call, so you need to take into account the distribution of data size of your
   *   records (i.e. avg and max).
   * @param pollSchedule
   *   Schedule for polling. The default schedule repeats immediately when there are more records available
   *   (millisBehindLatest > 0), otherwise it polls at a fixed interval of 1 second
   * @param throttlingBackoff
   *   When getting a Provisioned Throughput Exception or KmsThrottlingException, schedule to apply for backoff.
   *   Although zio-kinesis will make no more than 5 calls to GetRecords per second (the AWS limit), some limits depend
   *   on the size of the records being fetched.
   * @param retrySchedule
   *   Schedule for retrying in case of non-throttling related issues
   * @param bufferNrBatches
   *   The number of fetched batches (chunks) to buffer. A buffer allows downstream to process the records while a new
   *   poll call is being made concurrently. A batch will contain up to `batchSize` records. Prefer powers of 2 for this
   *   value for performance reasons.
   */
  final case class Polling(
    batchSize: Int = 1000,
    pollSchedule: Schedule[Any, GetRecordsResponse.ReadOnly, Any] = Polling.dynamicSchedule(1.second),
    throttlingBackoff: Schedule[Any, Any, (Duration, Long)] = Util.exponentialBackoff(5.seconds, 30.seconds),
    retrySchedule: Schedule[Any, Any, (Duration, Long)] = Util.exponentialBackoff(1.second, 1.minute),
    bufferNrBatches: Int = 2
  ) extends FetchMode

  object Polling {

    /**
     * Creates a polling schedule that immediately repeats when there are more records available (millisBehindLatest >
     * 0), otherwise polls at a fixed interval.
     *
     * @param interval
     *   Fixed interval for polling when no more records are currently available
     */
    def dynamicSchedule(interval: Duration): Schedule[Any, GetRecordsResponse.ReadOnly, Any] =
      (Schedule.recurWhile[Boolean](_ == true) || Schedule.fixed(interval))
        .contramap((_: GetRecordsResponse.ReadOnly).millisBehindLatest.getOrElse(0) != 0)
  }

  /**
   * Fetch data using enhanced fanout
   *
   * @param retrySchedule
   *   Schedule for retrying in case of connection issues
   */
  final case class EnhancedFanOut(
    deregisterConsumerAtShutdown: Boolean = true,
    maxSubscriptionsPerSecond: Int = 10,
    retrySchedule: Schedule[Any, Any, (Duration, Long)] = Util.exponentialBackoff(5.second, 1.minute)
  ) extends FetchMode
}

object Consumer {

  /**
   * Creates a stream that emits streams for each Kinesis shard that this worker holds a lease for.
   *
   * Upon initialization, a lease table is created if it does not yet exist. For each shard of the stream a lease is
   *
   * CHECKPOINTING
   *
   * Clients should periodically checkpoint their progress using the `Checkpointer`. Each processed record may be staged
   * with the Checkpointer to ensure that when the stream is interrupted, the last staged record will be checkpointed.
   *
   * When the stream is interrupted, the last staged checkpoint for each shard will be checkpointed and leases for that
   * shard are released.
   *
   * Checkpointing may fail by two (expected) causes:
   *   - Connection failures
   *   - Shard lease taken by another worker (see MULTIPLE WORKERS)
   *
   * In both cases, clients should end the shard stream by catching the error and continuing with an empty ZStream.
   *
   * MULTIPLE WORKERS
   *
   * Upon initialization, the Consumer will check existing leases to see how many workers are currently active. It will
   * immediately steal its fair share of leases from other workers, in such a way that all workers end up with a new
   * fair share of leases. When it is the only active worker, it will take all leases. The leases per worker are
   * randomized to reduce the chance of lease stealing contention.
   *
   * This procedure is safe against multiple workers initializing concurrently.
   *
   * Leases are periodically renewed and leases of other workers are refreshed. When another worker's lease has not been
   * updated for some time, it is considered expired and the worker considered a zombie. The new fair share of leases
   * for all workers is then determined and this worker will try to claim some of the expired leases.
   *
   * Each of the shard streams may end when the lease for that shard is lost.
   *
   * CONNECTION FAILURES
   *
   * The consumer will keep on running when there are connection issues to Kinesis or DynamoDB. An exponential backoff
   * schedule (user-customizable) is applied to retry in case of such failures. When leases expire due to being unable
   * to renew them under these circumstances, the shard lease is released and the shard stream is ended. When the
   * connection is restored, the lease coordinator will try to take leases again to get to the target number of leases.
   *
   * DIAGNOSTIC EVENTS An optional function `emitDiagnostic` can be passed to be called when interesting events happen
   * in the Consumer. This is useful for logging and for metrics.
   *
   * @param streamName
   *   Name of the kinesis stream
   * @param applicationName
   *   Name of the application. This is used as the table name for lease coordination (DynamoDB)
   * @param deserializer
   *   Record deserializer
   * @param workerIdentifier
   *   Identifier of this worker, used for lease coordination
   * @param fetchMode
   *   How to fetch records: Polling or EnhancedFanOut, including config parameters
   * @param leaseCoordinationSettings
   *   Config parameters for lease coordination
   * @param initialPosition
   *   When no checkpoint exists yet for a shard, start processing from this position
   * @param emitDiagnostic
   *   Function that is called for events happening in the Consumer. For diagnostics / metrics.
   * @param shardAssignmentStrategy
   *   How to assign shards to this worker
   * @tparam R
   * @tparam T
   *   Record type
   * @return
   *   Stream of tuples of (shard ID, shard stream, checkpointer)
   */
  def shardedStream[R, T](
    streamName: String,
    applicationName: String,
    deserializer: Deserializer[R, T],
    workerIdentifier: String = "worker1",
    fetchMode: FetchMode = FetchMode.Polling(),
    leaseCoordinationSettings: LeaseCoordinationSettings = LeaseCoordinationSettings(),
    initialPosition: InitialPosition = InitialPosition.TrimHorizon,
    emitDiagnostic: DiagnosticEvent => UIO[Unit] = _ => ZIO.unit,
    shardAssignmentStrategy: ShardAssignmentStrategy = ShardAssignmentStrategy.balanced()
  ): ZStream[
    Kinesis with LeaseRepository with R,
    Throwable,
    (
      String,
      ZStream[
        Any,
        Throwable,
        Record[T]
      ],
      Checkpointer
    )
  ] = {
    def toRecords(
      shardId: String,
      r: zio.aws.kinesis.model.Record.ReadOnly
    ): ZIO[R, Throwable, Chunk[Record[T]]] = {
      val dataChunk = r.data

      if (ProtobufAggregation.isAggregatedRecord(dataChunk))
        for {
          aggregatedRecord <- ZIO.fromTry(ProtobufAggregation.decodeAggregatedRecord(dataChunk))
          _                 = ZIO.logDebug(s"Found aggregated record with ${aggregatedRecord.getRecordsCount} sub records")
          records          <- ZIO.foreach(aggregatedRecord.getRecordsList.asScala.zipWithIndex.toSeq) {
                                case (subRecord, subSequenceNr) =>
                                  val data = Chunk.fromByteBuffer(subRecord.getData.asReadOnlyByteBuffer())

                                  deserializer
                                    .deserialize(data)
                                    .map { data =>
                                      Record(
                                        shardId,
                                        r.sequenceNumber,
                                        r.approximateArrivalTimestamp.toOption.get,
                                        data,
                                        aggregatedRecord.getPartitionKeyTable(subRecord.getPartitionKeyIndex.toInt),
                                        r.encryptionType.toOption,
                                        Some(subSequenceNr.toLong),
                                        if (subRecord.hasExplicitHashKeyIndex)
                                          Some(aggregatedRecord.getExplicitHashKeyTable(subRecord.getExplicitHashKeyIndex.toInt))
                                        else None,
                                        aggregated = true
                                      )
                                    }
                              }
        } yield Chunk.fromIterable(records)
      else
        deserializer
          .deserialize(r.data)
          .map { data =>
            Record(
              shardId,
              r.sequenceNumber,
              r.approximateArrivalTimestamp.toOption.get,
              data,
              r.partitionKey,
              r.encryptionType.toOption,
              subSequenceNumber = None,
              explicitHashKey = None,
              aggregated = false
            )
          }
          .map(Chunk.single)
    }

    def makeFetcher(
      streamDescription: StreamDescription.ReadOnly
    ): ZIO[Scope with Kinesis, Throwable, Fetcher] =
      fetchMode match {
        case c: Polling        => PollingFetcher.make(streamDescription.streamName, c, emitDiagnostic)
        case c: EnhancedFanOut => EnhancedFanOutFetcher.make(streamDescription, workerIdentifier, c, emitDiagnostic)
      }

    val listShards: ZIO[Kinesis, Throwable, Map[ShardId, Shard.ReadOnly]] = Kinesis
      .listShards(ListShardsRequest(streamName = Some(StreamName(streamName))))
      .mapError(_.toThrowable)
      .runCollect
      .map(_.map(l => (l.shardId, l)).toMap)
      .flatMap { shards =>
        if (shards.isEmpty) ZIO.fail(new Exception("No shards in stream!"))
        else ZIO.succeed(shards)
      }

    def createDependencies: ZIO[
      Kinesis with Scope with LeaseRepository,
      Throwable,
      (Fetcher, LeaseCoordinator)
    ] =
      Kinesis
        .describeStream(DescribeStreamRequest(StreamName(streamName)))
        .mapError(_.toThrowable)
        .map(_.streamDescription)
        .forkScoped // joined later
        .flatMap { streamDescriptionFib =>
          val fetchInitialShards = streamDescriptionFib.join.flatMap { streamDescription =>
            if (!streamDescription.hasMoreShards)
              ZIO.succeed(streamDescription.shards.map(s => s.shardId -> s).toMap)
            else
              listShards
          }

          streamDescriptionFib.join.flatMap(makeFetcher) zipPar (
            // Fetch shards and initialize the lease coordinator at the same time
            // When we have the shards, we inform the lease coordinator. When the lease table
            // still has to be created, we have the shards in time for lease claiming begins.
            // If not in time, the next cycle of takeLeases will take care of it
            // When the lease table already exists, the updateShards call will not provide
            // additional information to the lease coordinator, and the list of leases is used
            // as the list of shards.
            for {
              env              <- ZIO.environment[Kinesis]
              leaseCoordinator <- DefaultLeaseCoordinator
                                    .make(
                                      applicationName,
                                      workerIdentifier,
                                      emitDiagnostic,
                                      leaseCoordinationSettings,
                                      fetchInitialShards.provideEnvironment(env),
                                      listShards.provideEnvironment(env),
                                      shardAssignmentStrategy,
                                      initialPosition
                                    )
              _                <- ZIO.logInfo("Lease coordinator created")
            } yield leaseCoordinator
          )
        }

    ZStream.logAnnotate("worker", workerIdentifier) *>
      ZStream.unwrapScoped {
        createDependencies.map { case (fetcher, leaseCoordinator) =>
          leaseCoordinator.acquiredLeases.collect { case AcquiredLease(shardId, leaseLost) =>
            (shardId, leaseLost)
          }
            .mapZIOParUnordered(leaseCoordinationSettings.maxParallelLeaseAcquisitions) { case (shardId, leaseLost) =>
              for {
                checkpointer    <- leaseCoordinator.makeCheckpointer(shardId)
                env             <- ZIO.environment[R]
                checkpointOpt   <- leaseCoordinator.getCheckpointForShard(shardId)
                startingPosition = checkpointOpt
                                     .map(checkpointToStartingPosition(_, initialPosition))
                                     .getOrElse(InitialPosition.toStartingPosition(initialPosition))
                shardStream      = fetcher
                                     .shardRecordStream(ShardId(shardId), startingPosition)
                                     .catchAll {
                                       case Left(e)                            =>
                                         ZStream.fromZIO(
                                           ZIO.logSpan(s"Shard stream ${shardId} failed")(ZIO.logErrorCause(Cause.fail(e)))
                                         ) *>
                                           ZStream.fail(e)
                                       case Right(EndOfShard(childShards @ _)) =>
                                         ZStream.fromZIO(
                                           ZIO.logDebug(
                                             s"Found end of shard for ${shardId}. " +
                                               s"Child shards are ${childShards.map(_.shardId).mkString(", ")}"
                                           ) *>
                                             checkpointer.markEndOfShard() *>
                                             leaseCoordinator.childShardsDetected(childShards)
                                         ) *> ZStream.empty
                                     }
                                     .mapChunksZIO {
                                       _.mapZIO(record => toRecords(shardId, record))
                                         .map(_.flatten)
                                     }
                                     .dropWhile(r => !checkpointOpt.forall(aggregatedRecordIsAfterCheckpoint(r, _)))
                                     .mapChunksZIO { chunk =>
                                       chunk.lastOption
                                         .fold(ZIO.unit) { r =>
                                           val extendedSequenceNumber =
                                             ExtendedSequenceNumber(
                                               r.sequenceNumber,
                                               r.subSequenceNumber.getOrElse(0L)
                                             )
                                           checkpointer.setMaxSequenceNumber(extendedSequenceNumber)
                                         }
                                         .as(chunk)
                                     }
                                     .terminateOnPromiseCompleted(leaseLost)
              } yield (
                shardId,
                shardStream.ensuring {
                  checkpointer.checkpointAndRelease.catchAll {
                    case Left(e)               =>
                      ZIO.logWarning(s"Error in checkpoint and release: ${e}").unit
                    case Right(ShardLeaseLost) =>
                      ZIO.unit // This is fine during shutdown
                  }
                }.provideEnvironment(env),
                checkpointer
              )
            }
        }
      }
  }

  /**
   * Apply an effectful function to each record in a stream
   *
   * This is the easiest way to consume Kinesis records from a stream, while benefiting from all of Consumer's features
   * like parallel streaming, checkpointing and resharding.
   *
   * Simply provide an effectful function that is applied to each record and the rest is taken care of.
   * @param checkpointBatchSize
   *   Maximum number of records before checkpointing
   * @param checkpointDuration
   *   Maximum interval before checkpointing
   * @param recordProcessor
   *   A function for processing a `Record[T]`
   * @tparam R
   *   ZIO environment type required by the `deserializer` and the `recordProcessor`
   * @tparam T
   *   Type of record values
   * @return
   *   A ZIO that completes with Unit when record processing is stopped or fails when the consumer stream fails
   */
  def consumeWith[R, RC, T](
    streamName: String,
    applicationName: String,
    deserializer: Deserializer[R, T],
    workerIdentifier: String = "worker1",
    fetchMode: FetchMode = FetchMode.Polling(),
    leaseCoordinationSettings: LeaseCoordinationSettings = LeaseCoordinationSettings(),
    initialPosition: InitialPosition = InitialPosition.TrimHorizon,
    emitDiagnostic: DiagnosticEvent => UIO[Unit] = _ => ZIO.unit,
    shardAssignmentStrategy: ShardAssignmentStrategy = ShardAssignmentStrategy.balanced(),
    checkpointBatchSize: Long = 200,
    checkpointDuration: Duration = 5.minutes
  )(
    recordProcessor: Record[T] => RIO[RC, Unit]
  ): ZIO[
    R with RC with Kinesis with LeaseRepository,
    Throwable,
    Unit
  ] =
    for {
      _ <- shardedStream(
             streamName,
             applicationName,
             deserializer,
             workerIdentifier,
             fetchMode,
             leaseCoordinationSettings,
             initialPosition,
             emitDiagnostic,
             shardAssignmentStrategy
           ).flatMapPar(Int.MaxValue) { case (_, shardStream, checkpointer) =>
             shardStream
               .tap(record => recordProcessor(record) *> checkpointer.stage(record))
               .viaFunction(
                 checkpointer.checkpointBatched[RC](nr = checkpointBatchSize, interval = checkpointDuration)
               )
           }.runDrain
    } yield ()

  private[zionative] val isThrottlingException: PartialFunction[Throwable, Unit] = {
    case _: KmsThrottlingException                 => ()
    case _: ProvisionedThroughputExceededException => ()
    case _: LimitExceededException                 => ()
  }

  private[zionative] def retryOnThrottledWithSchedule[R, A](
    schedule: Schedule[R, Throwable, A]
  ): Schedule[R, Throwable, (Throwable, A)] =
    Schedule.recurWhile[Throwable](e => isThrottlingException.lift(e).isDefined) && schedule

  private[client] def childShardToShard(s: ChildShard.ReadOnly): Shard.ReadOnly = {
    val parentShards = s.parentShards

    val shard = Shard(
      s.shardId,
      hashKeyRange = s.hashKeyRange.asEditable,
      sequenceNumberRange = SequenceNumberRange(SequenceNumber("0"), None)
    )

    val shardWithParents =
      if (parentShards.size == 2)
        shard.copy(parentShardId = Some(parentShards.head), adjacentParentShardId = Some(parentShards(1)))
      else if (parentShards.size == 1)
        shard.copy(parentShardId = Some(parentShards.head))
      else
        throw new IllegalArgumentException(s"Unexpected nr of parent shards: ${parentShards.size}")

    shardWithParents.asReadOnly
  }

  val defaultEnvironment: ZLayer[Any, Throwable, Kinesis with LeaseRepository with CloudWatch] =
    HttpClientBuilder.make() >>> zio.aws.core.config.AwsConfig.default >>>
      (kinesisAsyncClientLayer() ++ (dynamoDbAsyncClientLayer() >>> DynamoDbLeaseRepository.live) ++ cloudWatchAsyncClientLayer())

  sealed trait InitialPosition
  object InitialPosition {
    case object Latest                         extends InitialPosition
    case object TrimHorizon                    extends InitialPosition
    case class AtTimestamp(timestamp: Instant) extends InitialPosition

    def toStartingPosition(p: InitialPosition): StartingPosition =
      p match {
        case InitialPosition.Latest                 => StartingPosition(ShardIteratorType.LATEST)
        case InitialPosition.TrimHorizon            => StartingPosition(ShardIteratorType.TRIM_HORIZON)
        case InitialPosition.AtTimestamp(timestamp) =>
          StartingPosition(ShardIteratorType.AT_TIMESTAMP, timestamp = Some(Timestamp(timestamp)))
      }
  }

  private[zionative] val checkpointToStartingPosition
    : (Either[SpecialCheckpoint, ExtendedSequenceNumber], InitialPosition) => StartingPosition = {
    case (Left(SpecialCheckpoint.TrimHorizon), _)                                      => StartingPosition(ShardIteratorType.TRIM_HORIZON)
    case (Left(SpecialCheckpoint.Latest), _)                                           => StartingPosition(ShardIteratorType.LATEST)
    case (Left(SpecialCheckpoint.AtTimestamp), InitialPosition.AtTimestamp(timestamp)) =>
      StartingPosition(ShardIteratorType.AT_TIMESTAMP, timestamp = Some(Timestamp(timestamp)))
    case (Right(s), _)                                                                 =>
      StartingPosition(
        ShardIteratorType.AT_SEQUENCE_NUMBER,
        sequenceNumber = Some(SequenceNumber(s.sequenceNumber))
      )
    case s @ _                                                                         =>
      throw new IllegalArgumentException(s"${s} is not a valid checkpoint as starting position")
  }

  private[zionative] def aggregatedRecordIsAfterCheckpoint(
    record: Record[_],
    checkpoint: Either[SpecialCheckpoint, ExtendedSequenceNumber]
  ): Boolean =
    (checkpoint, record.subSequenceNumber) match {
      case (Left(_), _)                                                                                      => true
      case (Right(ExtendedSequenceNumber(sequenceNumber, subSequenceNumber)), Some(recordSubSequenceNumber)) =>
        (BigInt(record.sequenceNumber) > BigInt(sequenceNumber)) ||
        (BigInt(record.sequenceNumber) == BigInt(sequenceNumber) && recordSubSequenceNumber > subSequenceNumber)
      case (Right(ExtendedSequenceNumber(sequenceNumber, _)), None)                                          =>
        BigInt(record.sequenceNumber) > BigInt(sequenceNumber)
    }
}
