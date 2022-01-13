package nl.vroste.zio.kinesis.client
import java.time.Instant
import zio.aws.kinesis
import zio.aws.kinesis.model.{ ListShardsRequest, ShardFilter, ShardFilterType }
import zio.aws.kinesis.Kinesis
import nl.vroste.zio.kinesis.client.Producer.ProduceResponse
import nl.vroste.zio.kinesis.client.producer.ProducerLive.ProduceRequest
import nl.vroste.zio.kinesis.client.producer._
import nl.vroste.zio.kinesis.client.serde.Serializer
import zio._
import zio.stream.ZSink
import zio.Clock
import zio.Clock.instant
import zio.aws.kinesis.model.primitives.StreamName

import java.security.MessageDigest

/**
 * Producer for Kinesis records
 *
 * Supports higher volume producing than making use of the kinesis client directly.
 *
 * Features:
 * - Batching of records into a single PutRecords calls to Kinesis for reduced IO overhead
 * - Retry requests with backoff on recoverable errors
 * - Retry individual records
 * - Rate limiting to respect shard capacity
 * - Dynamic throttling: when other systems are producing records, Producer will find a rate that optimizes
 *   the success rate while maintaining high throughput.
 *  - Aggregatting of small records into one Kinesis records.
 *
 * Records are batched for up to 500 records or 5MB of payload size,
 * whichever comes first. The latter two are Kinesis API limits.
 *
 * Individual records which cannot be produced due to Kinesis shard rate limits are retried.
 *
 * Inspired by https://docs.aws.amazon.com/streams/latest/dev/developing-producers-with-kpl.html and
 * https://aws.amazon.com/blogs/big-data/implementing-efficient-and-reliable-producers-with-the-amazon-kinesis-producer-library/
 *
 * Rate limits for the Kinesis PutRecords API (see https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html):
 * - 500 records per request
 * - Whole request max 5MB
 * - Each item max 1MB
 * - Each shard max 1000 records / s
 * - Each shard max 1MB / s
 */
trait Producer[T] {

  /**
   * Produce a single record
   *
   * Backpressures when too many requests are in flight
   *
   * @param r
   * @return Task that fails if the records fail to be produced with a non-recoverable error
   */
  def produce(r: ProducerRecord[T]): Task[ProduceResponse]

  /**
   * Backpressures when too many requests are in flight
   *
   * @return Task that fails if any of the records fail to be produced with a non-recoverable error
   */
  def produceChunk(chunk: Chunk[ProducerRecord[T]]): Task[Chunk[ProduceResponse]]

  /**
   * ZSink interface to the Producer
   */
  def sinkChunked: ZSink[Any, Throwable, Chunk[ProducerRecord[T]], Nothing, Unit] =
    ZSink.drain.contramapZIO(produceChunk)
}

/**
 *
 * @param bufferSize Maximum number of records to be queued for processing
 *                   When this number is reached, calls to `produce` or `produceChunk` will backpressure.
 * @param maxParallelRequests Maximum number of `PutRecords` calls that are in flight concurrently
 * @param backoffRequests
 * @param failedDelay
 * @param metricsInterval Interval at which metrics are published
 * @param updateShardInterval Interval at which the stream's shards are refreshed
 * @param aggregate Aggregate records
 *                  Enabling this setting can give higher throughput for small records, by working around
 *                  the 1000 records/s limit per shard.
 * @param allowedErrorRate The maximum allowed rate of errors before throttling is applied
 * @param md5DigestPoolSize Size of the pool of MessageDigest instances used for shard prediction. The MessageDigest
 *                          is too costly to instantiate for each record, hence a `ZPool` of them is used. This
 *                          MessageDigest is used in `produce` and `produceChunk` calls.
 */
final case class ProducerSettings(
  bufferSize: Int = 8192,
  maxParallelRequests: Int = 24,
  backoffRequests: Schedule[Clock, Throwable, Any] = Schedule.exponential(500.millis) && Schedule.recurs(5),
  failedDelay: Duration = 100.millis,
  metricsInterval: Duration = 30.seconds,
  updateShardInterval: Duration = 30.seconds,
  aggregate: Boolean = false,
  allowedErrorRate: Double = 0.05,
  md5DigestPoolSize: Int = 8
) {
  require(allowedErrorRate > 0 && allowedErrorRate <= 1.0, "allowedErrorRate must be between 0 and 1 (inclusive)")
}

object Producer {
  final case class ProduceResponse(shardId: String, sequenceNumber: String, attempts: Int, completed: Instant)

  /**
   * Create a Producer of `T` values to stream `streamName`
   *
   * @param streamName Stream to produce to
   * @param serializer Serializer for values of type T
   * @param settings
   * @param metricsCollector Periodically called with producer metrics
   * @tparam R Environment required by the serializer, usually Any
   * @tparam R1
   * @tparam T Type of values to produce
   * @return A Managed Producer
   */
  def make[R, R1, T](
    streamName: String,
    serializer: Serializer[R, T],
    settings: ProducerSettings = ProducerSettings(),
    metricsCollector: ProducerMetrics => ZIO[R1, Nothing, Unit] = (_: ProducerMetrics) => ZIO.unit
  ): ZManaged[R with R1 with Clock with Kinesis, Throwable, Producer[T]] =
    for {
      client          <- ZManaged.service[Kinesis]
      env             <- ZIO.environment[R with Clock].toManaged
      queue           <- zio.Queue.bounded[ProduceRequest](settings.bufferSize).toManagedWith(_.shutdown)
      currentMetrics  <- instant.map(CurrentMetrics.empty).flatMap(Ref.make(_)).toManaged
      shardMap        <- getShardMap(StreamName(streamName)).toManaged
      currentShardMap <- Ref.make(shardMap).toManaged
      inFlightCalls   <- Ref.make(0).toManaged
      md5Pool         <- ZPool.make(ZManaged.fromZIO(ZIO(MessageDigest.getInstance("MD5"))), settings.md5DigestPoolSize)
      failedQueue     <- zio.Queue.bounded[ProduceRequest](settings.bufferSize).toManagedWith(_.shutdown)

      triggerUpdateShards <- Util.periodicAndTriggerableOperation(
                               (ZIO.logDebug("Refreshing shard map") *>
                                 (getShardMap(StreamName(streamName)) flatMap currentShardMap.set) *>
                                 ZIO.logInfo("Shard map was refreshed"))
                                 .tapError(e => ZIO.logError(s"Error refreshing shard map: ${e}").ignore)
                                 .ignore,
                               settings.updateShardInterval
                             )
      throttler           <- ShardThrottler.make(allowedError = settings.allowedErrorRate)

      producer = new ProducerLive[R, R1, T](
                   client,
                   env,
                   queue,
                   failedQueue,
                   serializer,
                   currentMetrics,
                   currentShardMap,
                   settings,
                   StreamName(streamName),
                   metricsCollector,
                   settings.aggregate,
                   inFlightCalls,
                   triggerUpdateShards,
                   throttler,
                   md5Pool
                 )
      _       <- producer.runloop.forkManaged
      _       <- producer.metricsCollection.forkManaged.ensuring(producer.collectMetrics)
    } yield producer

  private def getShardMap(streamName: StreamName): ZIO[Clock with Kinesis, Throwable, ShardMap] = {
    val shardFilter = ShardFilter(ShardFilterType.AT_LATEST) // Currently open shards
    Kinesis
      .listShards(ListShardsRequest(Some(streamName), shardFilter = Some(shardFilter)))
      .mapError(_.toThrowable)
      .runCollect
      .flatMap(shards => instant.map(ShardMap.fromShards(shards, _)))
  }
}
