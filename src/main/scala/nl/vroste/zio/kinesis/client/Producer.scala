package nl.vroste.zio.kinesis.client

import java.time.Instant

import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.Producer.ProduceResponse
import nl.vroste.zio.kinesis.client.ProducerLive.{ CurrentMetrics, ProduceRequest, ShardMap }
import nl.vroste.zio.kinesis.client.serde.Serializer
import software.amazon.awssdk.services.kinesis.model.{ ShardFilter, ShardFilterType }
import zio._
import zio.clock.{ instant, Clock }
import zio.duration.{ Duration, _ }
import zio.logging._
import zio.stream.ZSink

/**
 * Producer for Kinesis records
 *
 * Supports higher volume producing than making use of the [[Client]] directly.
 *
 * Features:
 * - Batching of records into a single PutRecords calls to Kinesis for reduced IO overhead
 * - Retry requests with backoff on recoverable errors
 * - Retry individual records
 * - Rate limiting to respect shard capacity (TODO)
 *
 * Records are batched for up to 500 records or 5MB of payload size,
 * whichever comes first. The latter two are Kinesis API limits.
 *
 * Individual records which cannot be produced due to Kinesis shard rate limits are retried.
 *
 * Individual shard rate limiting is not yet implemented by this library.
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
  def produceChunk(chunk: Chunk[ProducerRecord[T]]): Task[Seq[ProduceResponse]]

  /**
   * ZSink interface to the Producer
   */
  def sinkChunked: ZSink[Any, Throwable, Chunk[ProducerRecord[T]], Nothing, Unit] =
    ZSink.drain.contramapM(produceChunk)
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
 */
final case class ProducerSettings(
  bufferSize: Int = 8192,
  maxParallelRequests: Int = 24,
  backoffRequests: Schedule[Clock, Throwable, Any] = Schedule.exponential(500.millis) && Schedule.recurs(5),
  failedDelay: Duration = 100.millis,
  metricsInterval: Duration = 30.seconds,
  updateShardInterval: Duration = 30.seconds,
  aggregate: Boolean = false
)

object Producer {
  final case class ProduceResponse(shardId: String, sequenceNumber: String, attempts: Int, completed: Instant)

  def make[R, R1, T](
    streamName: String,
    serializer: Serializer[R, T],
    settings: ProducerSettings = ProducerSettings(),
    metricsCollector: ProducerMetrics => ZIO[R1, Nothing, Unit] = (_: ProducerMetrics) => ZIO.unit
  ): ZManaged[R with R1 with Clock with Client with Logging, Throwable, Producer[T]] =
    for {
      client          <- ZManaged.service[Client.Service]
      env             <- ZIO.environment[R with Clock].toManaged_
      queue           <- zio.Queue.bounded[ProduceRequest](settings.bufferSize).toManaged(_.shutdown)
      currentMetrics  <- instant.map(CurrentMetrics.empty).flatMap(Ref.make).toManaged_
      shardMap        <- getShardMap(streamName).toManaged_
      currentShardMap <- Ref.make(shardMap).toManaged_
      inFlightCalls   <- Ref.make(0).toManaged_
      failedQueue     <- zio.Queue.bounded[ProduceRequest](settings.bufferSize).toManaged(_.shutdown)

      triggerUpdateShards <- Util.periodicAndTriggerableOperation(
                               (log.debug("Refreshing shard map") *>
                                 (getShardMap(streamName) >>= currentShardMap.set) *>
                                 log.info("Shard map was refreshed")).orDie,
                               settings.updateShardInterval
                             )

      producer             = new ProducerLive[R, R1, T](
                   client,
                   env,
                   queue,
                   failedQueue,
                   serializer,
                   currentMetrics,
                   currentShardMap,
                   settings,
                   streamName,
                   metricsCollector,
                   settings.aggregate,
                   inFlightCalls,
                   triggerUpdateShards
                 )
      _                   <- producer.runloop.forkManaged
      _                   <- producer.metricsCollection.forkManaged.ensuring(producer.collectMetrics)
    } yield producer

  private def getShardMap(streamName: String): ZIO[Clock with Client, Throwable, ShardMap] = {
    val shardFilter = ShardFilter.builder().`type`(ShardFilterType.AT_LATEST).build() // Currently open shards
    Client
      .listShards(streamName, filter = Some(shardFilter))
      .runCollect
      .flatMap(shards => instant.map(ShardMap.fromShards(shards, _)))
  }

}
