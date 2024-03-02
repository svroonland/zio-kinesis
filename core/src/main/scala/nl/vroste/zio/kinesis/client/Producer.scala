package nl.vroste.zio.kinesis.client
import nl.vroste.zio.kinesis.client.Producer.ProduceResponse
import nl.vroste.zio.kinesis.client.producer.ProducerLive.ProduceRequest
import nl.vroste.zio.kinesis.client.producer._
import nl.vroste.zio.kinesis.client.serde.Serializer
import zio.Clock.instant
import zio._
import zio.aws.kinesis.Kinesis
import zio.aws.kinesis.model.{ ListShardsRequest, ShardFilter, ShardFilterType }
import zio.stream.ZSink

import java.time.Instant
import java.security.MessageDigest

/**
 * Producer for Kinesis records
 *
 * Supports higher volume producing than making use of the kinesis client directly.
 *
 * Features:
 *   - Batching of records into a single PutRecords calls to Kinesis for reduced IO overhead
 *   - Retry requests with backoff on recoverable errors
 *   - Retry individual records
 *   - Rate limiting to respect shard capacity
 *   - Dynamic throttling: when other systems are producing records, Producer will find a rate that optimizes the
 *     success rate while maintaining high throughput.
 *     - Aggregatting of small records into one Kinesis records.
 *
 * Records are batched for up to 500 records or 5MB of payload size, whichever comes first. The latter two are Kinesis
 * API limits.
 *
 * Individual records which cannot be produced due to Kinesis shard rate limits are retried.
 *
 * Inspired by https://docs.aws.amazon.com/streams/latest/dev/developing-producers-with-kpl.html and
 * https://aws.amazon.com/blogs/big-data/implementing-efficient-and-reliable-producers-with-the-amazon-kinesis-producer-library/
 *
 * Rate limits for the Kinesis PutRecords API (see
 * https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html):
 *   - 500 records per request
 *   - Whole request max 5MB
 *   - Each item max 1MB
 *   - Each shard max 1000 records / s
 *   - Each shard max 1MB / s
 */
trait Producer[T] {

  /**
   * Produce a single record without awaiting the result.
   *
   * Backpressures when too many requests are in flight.
   *
   * @param r
   * @return
   *   Outer task completes when the record is queued for processing. Inner task completes when the record is produced
   *   or failed to be produced with a non-recoverable error.
   */
  def produceAsync(r: ProducerRecord[T]): Task[Task[ProduceResponse]]

  /**
   * Produce a single record.
   *
   * Backpressures when too many requests are in flight.
   *
   * @param r
   * @return
   *   Task that fails if the records fail to be produced with a non-recoverable error
   */
  final def produce(r: ProducerRecord[T]): Task[ProduceResponse] = produceAsync(r).flatten

  /**
   * Produce a chunk of records without awaiting the result.
   *
   * Backpressures when too many requests are in flight.
   *
   * @return
   *   Task that fails if any of the records fail to be produced with a non-recoverable error
   */
  def produceChunkAsync(chunk: Chunk[ProducerRecord[T]]): Task[Task[Chunk[ProduceResponse]]]

  /**
   * Produce a chunk of records.
   *
   * Backpressures when too many requests are in flight.
   *
   * @return
   *   Outer task completes when all records are queued for processing. Inner task completes when all records are
   *   produced or when a record failed to be produced with a non-recoverable error.
   */
  final def produceChunk(chunk: Chunk[ProducerRecord[T]]): Task[Chunk[ProduceResponse]] =
    produceChunkAsync(chunk).flatten

  /**
   * ZSink interface to the Producer
   */
  @deprecated("use `sink`", "0.30.1")
  def sinkChunked: ZSink[Any, Throwable, Chunk[ProducerRecord[T]], Nothing, Unit] =
    ZSink.drain.contramapZIO(produceChunk)

  /**
   * ZSink interface to the Producer
   */
  def sink: ZSink[Any, Throwable, ProducerRecord[T], Nothing, Unit] =
    ZSink.foreachChunk(produceChunk)
}

/**
 * @param bufferSize
 *   Maximum number of records to be queued for processing When this number is reached, calls to `produce` or
 *   `produceChunk` will backpressure.
 * @param maxParallelRequests
 *   Maximum number of `PutRecords` calls that are in flight concurrently
 * @param backoffRequests
 * @param failedDelay
 * @param metricsInterval
 *   Interval at which metrics are published
 * @param updateShardInterval
 *   Interval at which the stream's shards are refreshed
 * @param shardPrediction
 *   Settings for shard prediction
 * @param aggregation
 *   Settings for aggregation
 * @param throttling
 *   Throttling settings for per-shard throttling
 * @param batchDuration
 *   Max duration a batch is kept open before it is sent to Kinesis. If None, the batch is sent as soon as a worker
 *   becomes available. Setting this to a larger value will reduce the number of requests to Kinesis and decrease cpu
 *   usage, but will increase latency.
 */
final case class ProducerSettings(
  bufferSize: Int = 8192,
  maxParallelRequests: Int = 24,
  backoffRequests: Schedule[Any, Throwable, Any] = Schedule.exponential(500.millis) && Schedule.recurs(5),
  failedDelay: Duration = 100.millis,
  metricsInterval: Duration = 30.seconds,
  updateShardInterval: Duration = 30.seconds,
  shardPrediction: Producer.ShardPrediction = Producer.ShardPrediction.Enabled(),
  aggregation: Producer.Aggregation = Producer.Aggregation.Disabled,
  throttling: Producer.Throttling = Producer.Throttling.Enabled(),
  batchDuration: Option[Duration] = None
) {
  aggregation match {
    case Producer.Aggregation.ByPredictedShard(_) =>
      require(shardPrediction.isEnabled, "Aggregation requires shard prediction to be enabled")
    case _                                        => ()
  }

  throttling match {
    case Producer.Throttling.Enabled(_) =>
      require(shardPrediction.isEnabled, "Throttling requires shard prediction to be enabled")
    case _                              => ()
  }
}

object Producer {

  sealed trait Throttling extends Product with Serializable

  object Throttling {
    case object Disabled extends Throttling

    /**
     * @param allowedErrorRate
     *   The maximum allowed rate of errors before throttling is applied
     */
    final case class Enabled(allowedErrorRate: Double = 0.05) extends Throttling {
      require(allowedErrorRate > 0 && allowedErrorRate <= 1.0, "allowedErrorRate must be between 0 and 1 (inclusive)")
    }

  }

  sealed trait ShardPrediction extends Product with Serializable {
    def isEnabled: Boolean = this match {
      case ShardPrediction.Disabled   => false
      case _: ShardPrediction.Enabled => true
    }
  }

  object ShardPrediction {
    case object Disabled extends ShardPrediction

    /**
     * @param parallelism
     *   Max number of parallel shard predictions (MD5 hashing)
     */
    final case class Enabled(parallelism: Int = 8) extends ShardPrediction {
      require(parallelism > 0, "shardPredictionParallelism must be > 0")
    }
  }

  sealed trait Aggregation extends Product with Serializable

  object Aggregation {
    case object Disabled extends Aggregation

    /**
     * Aggregates records based on their predicted shard
     *
     * @param aggregationDuration
     *   Max duration for which an aggregate record is kept open. If None, the aggregate is closed as soon as downstream
     *   is ready to process it.
     */
    final case class ByPredictedShard(aggregationDuration: Option[Duration] = None) extends Aggregation
  }

  final case class ProduceResponse(shardId: String, sequenceNumber: String, attempts: Int, completed: Instant)

  private[client] sealed trait RichShardPrediction extends Product with Serializable

  private[client] object RichShardPrediction {
    case object Disabled extends RichShardPrediction

    final case class Enabled(
      parallelism: Int,
      md5Pool: ZPool[Nothing, MessageDigest],
      shards: Ref[ShardMap],
      triggerUpdateShards: UIO[Unit]
    ) extends RichShardPrediction
  }

  private[client] sealed trait RichThrottling extends Product with Serializable

  private[client] object RichThrottling {
    case object Disabled extends RichThrottling

    final case class Enabled(throttler: ShardThrottler) extends RichThrottling
  }

  /**
   * Create a Producer of `T` values to stream `streamIdentifier`
   *
   * @param streamIdentifier
   *   Stream to produce to. Either just the name or the whole arn.
   * @param serializer
   *   Serializer for values of type T
   * @param settings
   * @param metricsCollector
   *   Periodically called with producer metrics
   * @tparam R
   *   Environment required by the serializer, usually Any
   * @tparam R1
   * @tparam T
   *   Type of values to produce
   * @return
   *   A Managed Producer
   */
  def make[R, R1, T](
    streamIdentifier: StreamIdentifier,
    serializer: Serializer[R, T],
    settings: ProducerSettings = ProducerSettings(),
    metricsCollector: ProducerMetrics => ZIO[R1, Nothing, Unit] = (_: ProducerMetrics) => ZIO.unit
  ): ZIO[Scope with R with R1 with Kinesis, Throwable, Producer[T]] =
    for {
      client         <- ZIO.service[Kinesis]
      env            <- ZIO.environment[R]
      queue          <- ZIO.acquireRelease(Queue.bounded[ProduceRequest](settings.bufferSize))(_.shutdown)
      currentMetrics <- instant.map(CurrentMetrics.empty).flatMap(Ref.make(_))
      inFlightCalls  <- Ref.make(0)
      failedQueue    <- ZIO.acquireRelease(Queue.bounded[ProduceRequest](settings.bufferSize))(_.shutdown)

      richShardPrediction <- settings.shardPrediction match {
                               case ShardPrediction.Disabled                       => ZIO.succeed(RichShardPrediction.Disabled)
                               case ShardPrediction.Enabled(predictionParallelism) =>
                                 for {
                                   shardMap            <- getShardMap(streamIdentifier)
                                   currentShardMap     <- Ref.make(shardMap)
                                   triggerUpdateShards <- Util.periodicAndTriggerableOperation(
                                                            (ZIO.logDebug("Refreshing shard map") *>
                                                              (getShardMap(
                                                                streamIdentifier
                                                              ) flatMap currentShardMap.set) *>
                                                              ZIO.logInfo("Shard map was refreshed"))
                                                              .tapError(e =>
                                                                ZIO.logError(s"Error refreshing shard map: ${e}").ignore
                                                              )
                                                              .ignore,
                                                            settings.updateShardInterval
                                                          )
                                   md5Pool             <-
                                     ZPool.make(ShardMap.md5, settings.maxParallelRequests + predictionParallelism)
                                 } yield RichShardPrediction.Enabled(
                                   predictionParallelism,
                                   md5Pool,
                                   currentShardMap,
                                   triggerUpdateShards
                                 )
                             }

      richThrottling <- settings.throttling match {
                          case Throttling.Disabled                  =>
                            ZIO.succeed(RichThrottling.Disabled)
                          case Throttling.Enabled(allowedErrorRate) =>
                            ShardThrottler.make(allowedError = allowedErrorRate).map(RichThrottling.Enabled(_))
                        }

      producer = new ProducerLive[R, R1, T](
                   client,
                   env,
                   queue,
                   failedQueue,
                   serializer,
                   currentMetrics,
                   settings,
                   streamIdentifier,
                   metricsCollector,
                   inFlightCalls,
                   richShardPrediction,
                   richThrottling
                 )
      _       <- producer.runloop.forkScoped                                             // Fiber cannot fail
      _       <- producer.metricsCollection.ensuring(producer.collectMetrics).forkScoped // Fiber cannot fail
    } yield producer

  private def getShardMap(streamIdentifier: StreamIdentifier): ZIO[Kinesis, Throwable, ShardMap] = {
    val shardFilter = ShardFilter(ShardFilterType.AT_LATEST) // Currently open shards
    Kinesis
      .listShards(
        ListShardsRequest(
          shardFilter = Some(shardFilter),
          streamName = streamIdentifier.name,
          streamARN = streamIdentifier.arn
        )
      )
      .mapError(_.toThrowable)
      .runCollect
      .flatMap(shards => instant.map(ShardMap.fromShards(shards, _)))
  }
}
