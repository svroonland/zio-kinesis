package nl.vroste.zio.kinesis.client.zionative

import java.time.Instant

import io.github.vigoo.zioaws.cloudwatch.CloudWatch
import io.github.vigoo.zioaws.kinesis
import io.github.vigoo.zioaws.kinesis.Kinesis
import io.github.vigoo.zioaws.kinesis.model._
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
import zio.clock.Clock
import zio.duration._
import zio.logging.{ log, Logging }
import zio.random.Random
import zio.stream.ZStream

import scala.jdk.CollectionConverters._

final case class ExtendedSequenceNumber(sequenceNumber: String, subSequenceNumber: Long)

sealed trait FetchMode
object FetchMode {

  /**
   * Fetches records in a polling manner
   *
   * @param batchSize The maximum number of records to retrieve in one call to GetRecords. Note that Kinesis
   *        defines limits in terms of the maximum size in bytes of this call, so you need to take into account
   *        the distribution of data size of your records (i.e. avg and max).
   * @param pollSchedule Schedule for polling. The default schedule repeats immediately when there are more
   *                     records available (millisBehindLatest > 0), otherwise it polls at a fixed interval of 1 second
   * @param throttlingBackoff When getting a Provisioned Throughput Exception or KmsThrottlingException,
   *                          schedule to apply for backoff. Although zio-kinesis will make no more than 5 calls
   *                          to GetRecords per second (the AWS limit), some limits depend on the size of the records
   *                          being fetched.
   * @param retrySchedule Schedule for retrying in case of non-throttling related issues
   * @param bufferNrBatches The number of fetched batches (chunks) to buffer. A buffer allows downstream to process the
   *                        records while a new poll call is being made concurrently. A batch will contain
   *                        up to `batchSize` records. Prefer powers of 2 for this value for performance reasons.
   */
  final case class Polling(
    batchSize: Int = 1000,
    pollSchedule: Schedule[Clock, GetRecordsResponse.ReadOnly, Any] = Polling.dynamicSchedule(1.second),
    throttlingBackoff: Schedule[Clock, Any, (Duration, Long)] = Util.exponentialBackoff(5.seconds, 30.seconds),
    retrySchedule: Schedule[Clock, Any, (Duration, Long)] = Util.exponentialBackoff(1.second, 1.minute),
    bufferNrBatches: Int = 2
  ) extends FetchMode

  object Polling {

    /**
     * Creates a polling schedule that immediately repeats when there are more records available
     * (millisBehindLatest > 0), otherwise polls at a fixed interval.
     *
     * @param interval Fixed interval for polling when no more records are currently available
     */
    def dynamicSchedule(interval: Duration): Schedule[Clock, GetRecordsResponse.ReadOnly, Any] =
      (Schedule.recurWhile[Boolean](_ == true) || Schedule.fixed(interval))
        .contramap((_: GetRecordsResponse.ReadOnly).millisBehindLatestValue.getOrElse(0) != 0)
  }

  /**
   * Fetch data using enhanced fanout
   *
   * @param retrySchedule Schedule for retrying in case of connection issues
   */
  final case class EnhancedFanOut(
    deregisterConsumerAtShutdown: Boolean = false, // TODO
    maxSubscriptionsPerSecond: Int = 10,
    retrySchedule: Schedule[Clock, Any, (Duration, Long)] = Util.exponentialBackoff(5.second, 1.minute)
  ) extends FetchMode
}

object Consumer {

  /**
   * Creates a stream that emits streams for each Kinesis shard that this worker holds a lease for.
   *
   * Upon initialization, a lease table is created if it does not yet exist. For each shard of the stream
   * a lease is
   *
   * CHECKPOINTING
   *
   * Clients should periodically checkpoint their progress using the `Checkpointer`. Each processed record
   * may be staged with the Checkpointer to ensure that when the stream is interrupted, the last staged record
   * will be checkpointed.
   *
   * When the stream is interrupted, the last staged checkpoint for each shard will be checkpointed and
   * leases for that shard are released.
   *
   * Checkpointing may fail by two (expected) causes:
   * - Connection failures
   * - Shard lease taken by another worker (see MULTIPLE WORKERS)
   *
   * In both cases, clients should end the shard stream by catching the error and continuing with an empty ZStream.
   *
   * MULTIPLE WORKERS
   *
   * Upon initialization, the Consumer will check existing leases to see how many workers are currently active.
   * It will immediately steal its fair share of leases from other workers, in such a way that all workers end up with a
   * new fair share of leases. When it is the only active worker, it will take all leases. The leases per worker
   * are randomized to reduce the chance of lease stealing contention.
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
   * The consumer will keep on running when there are connection issues to Kinesis or DynamoDB. An exponential
   * backoff schedule (user-customizable) is applied to retry in case of such failures. When leases expire due
   * to being unable to renew them under these circumstances, the shard lease is released and the shard stream is
   * ended. When the connection is restored, the lease coordinator will try to take leases again to get to the
   * target number of leases.
   *
   * DIAGNOSTIC EVENTS
   * An optional function `emitDiagnostic` can be passed to be called when interesting events happen in the Consumer.
   * This is useful for logging and for metrics.
   *
   * @param streamName Name of the kinesis stream
   * @param applicationName Name of the application. This is used as the table name for lease coordination (DynamoDB)
   * @param deserializer Record deserializer
   * @param workerIdentifier Identifier of this worker, used for lease coordination
   * @param fetchMode How to fetch records: Polling or EnhancedFanOut, including config parameters
   * @param leaseCoordinationSettings Config parameters for lease coordination
   * @param initialPosition When no checkpoint exists yet for a shard, start processing from this position
   * @param emitDiagnostic Function that is called for events happening in the Consumer. For diagnostics / metrics.
   * @param shardAssignmentStrategy How to assign shards to this worker
   * @tparam R
   * @tparam T Record type
   * @return Stream of tuples of (shard ID, shard stream, checkpointer)
   */
  def shardedStream[R, T](
    streamName: String,
    applicationName: String,
    deserializer: Deserializer[R, T],
    workerIdentifier: String = "worker1",
    fetchMode: FetchMode = FetchMode.Polling(),
    leaseCoordinationSettings: LeaseCoordinationSettings = LeaseCoordinationSettings(),
    initialPosition: InitialPosition = InitialPosition.TrimHorizon,
    emitDiagnostic: DiagnosticEvent => UIO[Unit] = _ => UIO.unit,
    shardAssignmentStrategy: ShardAssignmentStrategy = ShardAssignmentStrategy.balanced()
  ): ZStream[
    Clock with Random with Kinesis with LeaseRepository with Logging with R,
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
      r: io.github.vigoo.zioaws.kinesis.model.Record.ReadOnly
    ): ZIO[Logging with R, Throwable, Chunk[Record[T]]] = {
      val dataChunk = r.dataValue

      if (ProtobufAggregation.isAggregatedRecord(dataChunk))
        for {
          aggregatedRecord <- ZIO.fromTry(ProtobufAggregation.decodeAggregatedRecord(dataChunk))
          _                 = log.debug(s"Found aggregated record with ${aggregatedRecord.getRecordsCount} sub records")
          records          <- ZIO.foreach(aggregatedRecord.getRecordsList.asScala.zipWithIndex.toSeq) {
                       case (subRecord, subSequenceNr) =>
                         val data = Chunk.fromByteBuffer(subRecord.getData.asReadOnlyByteBuffer())

                         deserializer
                           .deserialize(data)
                           .map { data =>
                             Record(
                               shardId,
                               r.sequenceNumberValue,
                               r.approximateArrivalTimestampValue.get,
                               data,
                               aggregatedRecord.getPartitionKeyTable(subRecord.getPartitionKeyIndex.toInt),
                               r.encryptionTypeValue,
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
          .deserialize(r.dataValue)
          .map { data =>
            Record(
              shardId,
              r.sequenceNumberValue,
              r.approximateArrivalTimestampValue.get,
              data,
              r.partitionKeyValue,
              r.encryptionTypeValue,
              subSequenceNumber = None,
              explicitHashKey = None,
              aggregated = false
            )
          }
          .map(Chunk.single)
    }

    def makeFetcher(
      streamDescription: StreamDescription.ReadOnly
    ): ZManaged[Clock with Kinesis with Logging, Throwable, Fetcher] =
      fetchMode match {
        case c: Polling        => PollingFetcher.make(streamDescription.streamNameValue, c, emitDiagnostic)
        case c: EnhancedFanOut => EnhancedFanOutFetcher.make(streamDescription, workerIdentifier, c, emitDiagnostic)
      }

    val listShards: ZIO[Kinesis, Throwable, Map[String, Shard.ReadOnly]] = kinesis
      .listShards(ListShardsRequest(streamName = Some(streamName)))
      .mapError(_.toThrowable)
      .runCollect
      .map(_.map(l => (l.shardIdValue, l)).toMap)
      .flatMap { shards =>
        if (shards.isEmpty) ZIO.fail(new Exception("No shards in stream!"))
        else ZIO.succeed(shards)
      }

    def createDependencies =
      ZManaged
        .fromEffect(
          kinesis
            .describeStream(DescribeStreamRequest(streamName))
            .mapError(_.toThrowable)
            .map(_.streamDescriptionValue)
            .fork
        )
        .flatMap { streamDescriptionFib =>
          val fetchShards = streamDescriptionFib.join.flatMap { streamDescription =>
            if (!streamDescription.hasMoreShardsValue)
              ZIO.succeed(streamDescription.shardsValue.map(s => s.shardIdValue -> s).toMap)
            else
              listShards
          }

          ZManaged
            .mapParN(
              ZManaged
                .unwrap(streamDescriptionFib.join.map(makeFetcher))
                .ensuring(log.debug("Fetcher shut down")),
              // Fetch shards and initialize the lease coordinator at the same time
              // When we have the shards, we inform the lease coordinator. When the lease table
              // still has to be created, we have the shards in time for lease claiming begins.
              // If not in time, the next cycle of takeLeases will take care of it
              // When the lease table already exists, the updateShards call will not provide
              // additional information to the lease coordinator, and the list of leases is used
              // as the list of shards.
              for {
                env              <- ZIO.environment[Clock with Kinesis].toManaged_
                leaseCoordinator <- DefaultLeaseCoordinator
                                      .make(
                                        applicationName,
                                        workerIdentifier,
                                        emitDiagnostic,
                                        leaseCoordinationSettings,
                                        fetchShards.provide(env),
                                        shardAssignmentStrategy,
                                        initialPosition
                                      )
                _                <- log.info("Lease coordinator created").toManaged_
                // Periodically refresh shards
                _                <- (listShards >>= leaseCoordinator.updateShards)
                       .repeat(Schedule.spaced(leaseCoordinationSettings.shardRefreshInterval))
                       .delay(leaseCoordinationSettings.shardRefreshInterval)
                       .forkManaged
              } yield leaseCoordinator
            )(_ -> _)
        }

    ZStream.unwrapManaged {
      createDependencies.map {
        case (fetcher, leaseCoordinator) =>
          leaseCoordinator.acquiredLeases.collect {
            case AcquiredLease(shardId, leaseLost) =>
              (shardId, leaseLost)
          }.mapMParUnordered(leaseCoordinationSettings.maxParallelLeaseAcquisitions) {
            case (shardId, leaseLost) =>
              for {
                checkpointer    <- leaseCoordinator.makeCheckpointer(shardId)
                env             <- ZIO.environment[Logging with Clock with Random with R]
                checkpointOpt   <- leaseCoordinator.getCheckpointForShard(shardId)
                startingPosition = checkpointOpt
                                     .map(checkpointToStartingPosition(_, initialPosition))
                                     .getOrElse(InitialPosition.toStartingPosition(initialPosition))
                stop             = ZStream.fromEffect(leaseLost.await).as(Exit.fail(None))
                shardStream      = (stop mergeTerminateEither fetcher
                                  .shardRecordStream(shardId, startingPosition)
                                  .catchAll {
                                    case Left(e)                            =>
                                      ZStream.fromEffect(log.error(s"Shard stream ${shardId} failed", Cause.fail(e))) *>
                                        ZStream.fail(e)
                                    case Right(EndOfShard(childShards @ _)) =>
                                      ZStream.fromEffect(
                                        log.debug(
                                          s"Found end of shard for ${shardId}. " +
                                            s"Child shards are ${childShards.map(_.shardId).mkString(", ")}"
                                        ) *>
                                          checkpointer.markEndOfShard() *>
                                          leaseCoordinator.childShardsDetected(childShards)
                                      ) *> ZStream.empty
                                  }
                                  .mapChunksM { chunk => // mapM is slow
                                    chunk
                                      .mapM(record => toRecords(shardId, record))
                                      .map(_.flatten)
                                      .tap { records =>
                                        records.lastOption.fold(ZIO.unit) { r =>
                                          val extendedSequenceNumber =
                                            ExtendedSequenceNumber(
                                              r.sequenceNumber,
                                              r.subSequenceNumber.getOrElse(0L)
                                            )
                                          checkpointer.setMaxSequenceNumber(extendedSequenceNumber)
                                        }
                                      }
                                  }
                                  .dropWhile(r => !checkpointOpt.forall(aggregatedRecordIsAfterCheckpoint(r, _)))
                                  .map(Exit.succeed(_))).flattenExitOption
              } yield (
                shardId,
                shardStream.ensuringFirst {
                  checkpointer.checkpointAndRelease.catchAll {
                    case Left(e)               =>
                      log.warn(s"Error in checkpoint and release: ${e}").unit
                    case Right(ShardLeaseLost) =>
                      ZIO.unit // This is fine during shutdown
                  }
                }.provide(env),
                checkpointer
              )
          }
      }
    }
  }

  /**
   * Apply an effectful function to each record in a stream
   *
   * This is the easiest way to consume Kinesis records from a stream, while benefiting from all of
   * Consumer's features like parallel streaming, checkpointing and resharding.
   *
   * Simply provide an effectful function that is applied to each record and the rest is taken care of.
   * @param checkpointBatchSize Maximum number of records before checkpointing
   * @param checkpointDuration Maximum interval before checkpointing
   * @param recordProcessor A function for processing a `Record[T]`
   * @tparam R ZIO environment type required by the `deserializer` and the `recordProcessor`
   * @tparam T Type of record values
   * @return A ZIO that completes with Unit when record processing is stopped or fails when the consumer stream fails
   */
  def consumeWith[R, RC, T](
    streamName: String,
    applicationName: String,
    deserializer: Deserializer[R, T],
    workerIdentifier: String = "worker1",
    fetchMode: FetchMode = FetchMode.Polling(),
    leaseCoordinationSettings: LeaseCoordinationSettings = LeaseCoordinationSettings(),
    initialPosition: InitialPosition = InitialPosition.TrimHorizon,
    emitDiagnostic: DiagnosticEvent => UIO[Unit] = _ => UIO.unit,
    shardAssignmentStrategy: ShardAssignmentStrategy = ShardAssignmentStrategy.balanced(),
    checkpointBatchSize: Long = 200,
    checkpointDuration: Duration = 5.minutes
  )(
    recordProcessor: Record[T] => RIO[RC, Unit]
  ): ZIO[
    R with RC with Clock with Random with Kinesis with LeaseRepository with Logging,
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
           ).flatMapPar(Int.MaxValue) {
               case (_, shardStream, checkpointer) =>
                 shardStream
                   .tap(record => recordProcessor(record) *> checkpointer.stage(record))
                   .via(checkpointer.checkpointBatched[RC](nr = checkpointBatchSize, interval = checkpointDuration))
             }
             .runDrain
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
    val parentShards = s.parentShardsValue

    val shard = Shard(
      s.shardIdValue,
      hashKeyRange = s.hashKeyRangeValue.editable,
      sequenceNumberRange = SequenceNumberRange("0", None)
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
    HttpClientBuilder.make() >>> io.github.vigoo.zioaws.core.config.default >>>
      (kinesisAsyncClientLayer() ++ (dynamoDbAsyncClientLayer() >>> DynamoDbLeaseRepository.live) ++ cloudWatchAsyncClientLayer())

  sealed trait InitialPosition
  object InitialPosition {
    final case object Latest                         extends InitialPosition
    final case object TrimHorizon                    extends InitialPosition
    final case class AtTimestamp(timestamp: Instant) extends InitialPosition

    def toStartingPosition(p: InitialPosition): StartingPosition =
      p match {
        case InitialPosition.Latest                 => StartingPosition(ShardIteratorType.LATEST)
        case InitialPosition.TrimHorizon            => StartingPosition(ShardIteratorType.TRIM_HORIZON)
        case InitialPosition.AtTimestamp(timestamp) =>
          StartingPosition(ShardIteratorType.AT_TIMESTAMP, timestamp = Some(timestamp))
      }
  }

  private[zionative] val checkpointToStartingPosition
    : (Either[SpecialCheckpoint, ExtendedSequenceNumber], InitialPosition) => StartingPosition = {
    case (Left(SpecialCheckpoint.TrimHorizon), _)                                      => StartingPosition(ShardIteratorType.TRIM_HORIZON)
    case (Left(SpecialCheckpoint.Latest), _)                                           => StartingPosition(ShardIteratorType.LATEST)
    case (Left(SpecialCheckpoint.AtTimestamp), InitialPosition.AtTimestamp(timestamp)) =>
      StartingPosition(ShardIteratorType.AT_TIMESTAMP, timestamp = Some(timestamp))
    case (Right(s), _)                                                                 =>
      StartingPosition(
        ShardIteratorType.AT_SEQUENCE_NUMBER,
        sequenceNumber = Some(s.sequenceNumber)
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
