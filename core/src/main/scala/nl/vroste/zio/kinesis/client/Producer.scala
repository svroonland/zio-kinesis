package nl.vroste.zio.kinesis.client
import nl.vroste.zio.kinesis.client.Producer.ProduceResponse
import nl.vroste.zio.kinesis.client.producer.ProducerLive.ProduceRequest
import nl.vroste.zio.kinesis.client.producer._
import nl.vroste.zio.kinesis.client.serde.Serializer
import zio.Clock.instant
import zio._
import zio.aws.kinesis.Kinesis
import zio.aws.kinesis.model.primitives.StreamName
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

  def produceAsync(r: ProducerRecord[T]): Task[Task[ProduceResponse]]

  /**
   * Produce a single record
   *
   * Backpressures when too many requests are in flight
   *
   * @param r
   * @return
   *   Task that fails if the records fail to be produced with a non-recoverable error
   */
  final def produce(r: ProducerRecord[T]): Task[ProduceResponse] = produceAsync(r).flatten

  def produceChunkAsync(chunk: Chunk[ProducerRecord[T]]): Task[Task[Chunk[ProduceResponse]]]

  /**
   * Backpressures when too many requests are in flight
   *
   * @return
   *   Task that fails if any of the records fail to be produced with a non-recoverable error
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
 * @param aggregate
 *   Aggregate records Enabling this setting can give higher throughput for small records, by working around the 1000
 *   records/s limit per shard.
 * @param allowedErrorRate
 *   The maximum allowed rate of errors before throttling is applied
 * @param shardPredictionParallelism
 *   Max number of parallel shard predictions (MD5 hashing)
 */
final case class ProducerSettings(
  bufferSize: Int = 8192,
  maxParallelRequests: Int = 24,
  backoffRequests: Schedule[Any, Throwable, Any] = Schedule.exponential(500.millis) && Schedule.recurs(5),
  failedDelay: Duration = 100.millis,
  metricsInterval: Duration = 30.seconds,
  updateShardInterval: Duration = 30.seconds,
  allowedErrorRate: Double = 0.05,
  shardPrediction: Producer.ShardPrediction = Producer.ShardPrediction.Enabled(8),
  aggregation: Producer.Aggregation = Producer.Aggregation.Disabled,
  batchDuration: Option[Duration] = None
) {
  require(allowedErrorRate > 0 && allowedErrorRate <= 1.0, "allowedErrorRate must be between 0 and 1 (inclusive)")

  aggregation match {
    case Producer.Aggregation.ByPredictedShard(_) =>
      require(aggregation != Producer.Aggregation.Disabled, "Aggregation requires shard prediction to be enabled")
    case Producer.Aggregation.Disabled            => ()
  }

  shardPrediction match {
    case Producer.ShardPrediction.Enabled(parallelism) =>
      require(parallelism > 0, "shardPredictionParallelism must be > 0")
    case Producer.ShardPrediction.Disabled             => ()
  }
}

object Producer {
  sealed trait ShardPrediction extends Product with Serializable
  object ShardPrediction {
    case object Disabled                           extends ShardPrediction
    final case class Enabled(parallelism: Int = 8) extends ShardPrediction
  }

  sealed trait RichShardPrediction extends Product with Serializable

  object RichShardPrediction {
    case object Disabled extends RichShardPrediction
    final case class Enabled(
      parallelism: Int,
      throttler: ShardThrottler,
      md5Pool: ZPool[Nothing, MessageDigest],
      shards: Ref[ShardMap],
      triggerUpdateShards: UIO[Unit]
    ) extends RichShardPrediction
  }

  sealed trait Aggregation extends Product with Serializable

  object Aggregation {
    case object Disabled                                                            extends Aggregation
    final case class ByPredictedShard(aggregationDuration: Option[Duration] = None) extends Aggregation
  }

  final case class ProduceResponse(shardId: String, sequenceNumber: String, attempts: Int, completed: Instant)

  /**
   * Create a Producer of `T` values to stream `streamName`
   *
   * @param streamName
   *   Stream to produce to
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
    streamName: String,
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
                                   shardMap            <- getShardMap(StreamName(streamName))
                                   currentShardMap     <- Ref.make(shardMap)
                                   triggerUpdateShards <- Util.periodicAndTriggerableOperation(
                                                            (ZIO.logDebug("Refreshing shard map") *>
                                                              (getShardMap(
                                                                StreamName(streamName)
                                                              ) flatMap currentShardMap.set) *>
                                                              ZIO.logInfo("Shard map was refreshed"))
                                                              .tapError(e =>
                                                                ZIO.logError(s"Error refreshing shard map: ${e}").ignore
                                                              )
                                                              .ignore,
                                                            settings.updateShardInterval
                                                          )
                                   throttler           <- ShardThrottler.make(allowedError = settings.allowedErrorRate)
                                   md5Pool             <-
                                     ZPool.make(ShardMap.md5, settings.maxParallelRequests + predictionParallelism)
                                 } yield RichShardPrediction.Enabled(
                                   predictionParallelism,
                                   throttler,
                                   md5Pool,
                                   currentShardMap,
                                   triggerUpdateShards
                                 )
                             }

      producer = new ProducerLive[R, R1, T](
                   client,
                   env,
                   queue,
                   failedQueue,
                   serializer,
                   currentMetrics,
                   settings,
                   StreamName(streamName),
                   metricsCollector,
                   inFlightCalls,
                   richShardPrediction
                 )
      _       <- producer.runloop.forkScoped                                             // Fiber cannot fail
      _       <- producer.metricsCollection.ensuring(producer.collectMetrics).forkScoped // Fiber cannot fail
    } yield producer

  private def getShardMap(streamName: StreamName): ZIO[Kinesis, Throwable, ShardMap] = {
    val shardFilter = ShardFilter(ShardFilterType.AT_LATEST) // Currently open shards
    Kinesis
      .listShards(ListShardsRequest(Some(streamName), shardFilter = Some(shardFilter)))
      .mapError(_.toThrowable)
      .runCollect
      .flatMap(shards => instant.map(ShardMap.fromShards(shards, _)))
  }
}
