package nl.vroste.zio.kinesis.client.zionative

import nl.vroste.zio.kinesis.client.AdminClient.StreamDescription
import nl.vroste.zio.kinesis.client.Client.ShardIteratorType
import nl.vroste.zio.kinesis.client.serde.Deserializer
import nl.vroste.zio.kinesis.client.zionative.FetchMode.{ EnhancedFanOut, Polling }
import nl.vroste.zio.kinesis.client.zionative.Fetcher.EndOfShard
import nl.vroste.zio.kinesis.client.zionative.LeaseCoordinator.AcquiredLease
import nl.vroste.zio.kinesis.client.zionative.fetcher.{ EnhancedFanOutFetcher, PollingFetcher }
import nl.vroste.zio.kinesis.client.zionative.leasecoordinator.{ DefaultLeaseCoordinator, LeaseCoordinationSettings }
import nl.vroste.zio.kinesis.client.zionative.leaserepository.DynamoDbLeaseRepository
import nl.vroste.zio.kinesis.client.{ sdkClientsLayer, AdminClient, Client, HttpClient, Record, Util }
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.kinesis.model.{
  GetRecordsResponse,
  KmsThrottlingException,
  LimitExceededException,
  ProvisionedThroughputExceededException,
  Shard,
  Record => KinesisRecord
}
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._
import zio.logging.{ log, Logging }
import zio.random.Random
import zio.stream.ZStream

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
    pollSchedule: Schedule[Clock, GetRecordsResponse, Any] = Polling.dynamicSchedule(1.second),
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
    def dynamicSchedule(interval: Duration): Schedule[Clock, GetRecordsResponse, Any] =
      (Schedule.recurWhile[Boolean](_ == true) || Schedule.fixed(interval))
        .contramap((_: GetRecordsResponse).millisBehindLatest() != 0)
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
    initialPosition: ShardIteratorType = ShardIteratorType.TrimHorizon,
    emitDiagnostic: DiagnosticEvent => UIO[Unit] = _ => UIO.unit,
    shardAssignmentStrategy: ShardAssignmentStrategy = ShardAssignmentStrategy.balanced()
  ): ZStream[
    Blocking with Clock with Random with Client with AdminClient with LeaseRepository with Logging with R,
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
    def toRecord(shardId: String, r: KinesisRecord): ZIO[R, Throwable, Record[T]] =
      deserializer.deserialize(r.data.asByteBuffer()).map { data =>
        Record(
          shardId,
          r.sequenceNumber,
          r.approximateArrivalTimestamp,
          data,
          r.partitionKey,
          r.encryptionType,
          0,    // r.subSequenceNumber,
          "",   // r.explicitHashKey,
          false //r.aggregated,
        )
      }

    def makeFetcher(
      streamDescription: StreamDescription
    ): ZManaged[Clock with Client with Logging, Throwable, Fetcher] =
      fetchMode match {
        case c: Polling        => PollingFetcher.make(streamDescription.streamName, c, emitDiagnostic)
        case c: EnhancedFanOut => EnhancedFanOutFetcher.make(streamDescription, workerIdentifier, c, emitDiagnostic)
      }

    val listShards: ZIO[Clock with Client, Throwable, Map[String, Shard]] = Client
      .listShards(streamName)
      .runCollect
      .map(_.map(l => (l.shardId(), l)).toMap)
      .flatMap { shards =>
        if (shards.isEmpty) ZIO.fail(new Exception("No shards in stream!"))
        else ZIO.succeed(shards)
      }

    def createDependencies =
      ZManaged
        .mapParN(
          ZManaged
            .unwrap(
              AdminClient
                .describeStream(streamName)
                .map(makeFetcher)
            )
            .ensuring(log.debug("Fetcher shut down")),
          // Fetch shards and initialize the lease coordinator at the same time
          // When we have the shards, we inform the lease coordinator. When the lease table
          // still has to be created, we have the shards in time for lease claiming begins.
          // If not in time, the next cycle of takeLeases will take care of it
          // When the lease table already exists, the updateShards call will not provide
          // additional information to the lease coordinator, and the list of leases is used
          // as the list of shards.
          for {
            fetchShards      <- listShards.forkManaged
            leaseCoordinator <- DefaultLeaseCoordinator
                                  .make(
                                    applicationName,
                                    workerIdentifier,
                                    emitDiagnostic,
                                    leaseCoordinationSettings,
                                    fetchShards.join,
                                    shardAssignmentStrategy
                                  )
            // Periodically refresh shards
            _                <- (listShards >>= leaseCoordinator.updateShards)
                   .repeat(Schedule.spaced(leaseCoordinationSettings.shardRefreshInterval))
                   .delay(leaseCoordinationSettings.shardRefreshInterval)
                   .forkManaged
          } yield leaseCoordinator
        )(_ -> _)

    ZStream.unwrapManaged {
      createDependencies.map {
        case (fetcher, leaseCoordinator) =>
          leaseCoordinator.acquiredLeases.collect {
            case AcquiredLease(shardId, leaseLost) =>
              (shardId, leaseLost)
          }.mapMParUnordered(leaseCoordinationSettings.maxParallelLeaseAcquisitions) {
            case (shardId, leaseLost) =>
              for {
                checkpointer        <- leaseCoordinator.makeCheckpointer(shardId)
                env                 <- ZIO.environment[Blocking with Logging with Clock with R]
                startingPositionOpt <- leaseCoordinator.getCheckpointForShard(shardId)
                startingPosition     = startingPositionOpt.map {
                                     case Left(SpecialCheckpoint.TrimHorizon) => ShardIteratorType.TrimHorizon
                                     case Left(SpecialCheckpoint.ShardEnd)    =>
                                       throw new IllegalArgumentException(
                                         s"SHARD_END is not a valid starting checkpoint for shard ${shardId}"
                                       )
                                     case Right(s)                            =>
                                       ShardIteratorType.AfterSequenceNumber(s.sequenceNumber)
                                   }.getOrElse(initialPosition)
                stop                 = ZStream.fromEffect(leaseLost.await).as(Exit.fail(None))
                shardStream          = (stop mergeTerminateEither fetcher
                                  .shardRecordStream(shardId, startingPosition)
                                  .catchAll {
                                    case Left(e)                            =>
                                      ZStream.fromEffect(log.error(s"Shard stream ${shardId} failed", Cause.fail(e))) *>
                                        ZStream.fail(e)
                                    case Right(EndOfShard(childShards @ _)) =>
                                      ZStream.fromEffect(
                                        log.debug(s"Found end of shard for ${shardId}!!") *>
                                          // TODO can we pick up leases for this shard somehow?
                                          checkpointer.markEndOfShard()
                                      ) *> ZStream.empty
                                  }
                                  .mapChunksM { chunk => // mapM is slow
                                    chunk
                                      .mapM(record => toRecord(shardId, record))
                                      .tap { records =>
                                        records.lastOption.fold(ZIO.unit) { r =>
                                          val extendedSequenceNumber =
                                            ExtendedSequenceNumber(r.sequenceNumber, r.subSequenceNumber)
                                          checkpointer.setMaxSequenceNumber(extendedSequenceNumber)
                                        }
                                      }
                                  }
                                  .map(Exit.succeed(_))).collectWhileSuccess
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

  private[zionative] val isThrottlingException: PartialFunction[Throwable, Unit] = {
    case _: KmsThrottlingException                 => ()
    case _: ProvisionedThroughputExceededException => ()
    case _: LimitExceededException                 => ()
  }

  private[zionative] def retryOnThrottledWithSchedule[R, A](
    schedule: Schedule[R, Throwable, A]
  ): Schedule[R, Throwable, (Throwable, A)] =
    Schedule.recurWhile[Throwable](e => isThrottlingException.lift(e).isDefined) && schedule

  val defaultEnvironment
    : ZLayer[Any, Throwable, AdminClient with Client with LeaseRepository with Has[CloudWatchAsyncClient]] =
    HttpClient.make() >>>
      sdkClientsLayer >+>
      (AdminClient.live ++ Client.live ++ DynamoDbLeaseRepository.live)

}
