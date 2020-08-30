package nl.vroste.zio.kinesis.client
import java.io.IOException
import java.time.Instant

import io.netty.handler.timeout.ReadTimeoutException
import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.Producer.ProduceResponse
import nl.vroste.zio.kinesis.client.serde.Serializer
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.model.{ KinesisException, PutRecordsRequestEntry }
import zio._
import zio.clock.{ instant, Clock }
import zio.duration.{ Duration, _ }
import zio.logging._
import zio.stream.{ ZSink, ZStream, ZTransducer }

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

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

final case class ProducerSettings(
  bufferSize: Int = 8192,
  maxParallelRequests: Int = 24,
  backoffRequests: Schedule[Clock, Throwable, Any] = Schedule.exponential(500.millis) && Schedule.recurs(5),
  failedDelay: Duration = 100.millis,
  metricsInterval: Duration = 1.second
)

final case class ProducerMetrics(
  timestamp: Instant,
  nrRecordsPublished: Long,
  nrRecordsFailed: Long,
  meanLatency: Option[Duration]
)

object Producer {
  def make[R, R1, T](
    streamName: String,
    serializer: Serializer[R, T],
    settings: ProducerSettings = ProducerSettings(),
    metricsCollector: ProducerMetrics => ZIO[R1, Nothing, Unit] = (_: ProducerMetrics) => ZIO.unit
  ): ZManaged[R with R1 with Clock with Client with Logging, Throwable, Producer[T]] =
    for {
      client         <- ZManaged.service[Client.Service]
      env            <- ZIO.environment[R with Clock].toManaged_
      queue          <- zio.Queue.bounded[ProduceRequest](settings.bufferSize).toManaged(_.shutdown)
      currentMetrics <- Ref.make(CurrentMetrics()).toManaged_

      failedQueue <- zio.Queue.bounded[ProduceRequest](settings.bufferSize).toManaged(_.shutdown)

      // Failed records get precedence)
      _ <- (ZStream
               .tick(100.millis)
               .mapConcatChunkM(_ => failedQueue.takeBetween(1, Int.MaxValue).map(Chunk.fromIterable)) merge ZStream
               .fromQueue(queue))
           // Batch records up to the Kinesis PutRecords request limits as long as downstream is busy
             .aggregateAsync(
               ZTransducer.fold(PutRecordsBatch.empty)(_.isWithinLimits)(_.add(_))
             )
             // Several putRecords requests in parallel
             .mapMParUnordered(settings.maxParallelRequests) { batch: PutRecordsBatch =>
               (for {
                 _                     <- log.info(s"Producing batch of size ${batch.nrRecords}")
                 response              <- client
                               .putRecords(streamName, batch.entriesInOrder.map(_.r))
                               .tapError(e => log.warn(s"Error producing records, will retry if recoverable: $e"))
                               .retry(scheduleCatchRecoverable && settings.backoffRequests)

                 maybeSucceeded         = response
                                    .records()
                                    .asScala
                                    .zip(batch.entriesInOrder)
                 (newFailed, succeeded) = if (response.failedRecordCount() > 0)
                                            maybeSucceeded.partition {
                                              case (result, _) =>
                                                result.errorCode() != null && recoverableErrorCodes.contains(
                                                  result.errorCode()
                                                )
                                            }
                                          else
                                            (Seq.empty, maybeSucceeded)

                 // Publish metrics
                 now                   <- instant
                 _                     <- currentMetrics.update(
                        _.add(
                          succeeded.size.toLong,
                          newFailed.size.toLong,
                          succeeded
                            .map(_._2.timestamp)
                            .map(timestamp => java.time.Duration.between(timestamp, now))
                        )
                      )
                 _                     <- log.info(s"Failed to produce ${newFailed.size} records").when(newFailed.nonEmpty)
                 // TODO backoff for shard limit stuff
                 _                     <- failedQueue
                        .offerAll(newFailed.map(_._2))
                        .when(newFailed.nonEmpty) // TODO should be per shard
                 _                     <- ZIO.foreach_(succeeded) {
                        case (response, request) =>
                          request.done.completeWith(
                            ZIO.succeed(ProduceResponse(response.shardId(), response.sequenceNumber()))
                          )
                      }
               } yield ()).catchAll {
                 case NonFatal(e) =>
                   log.warn("Failed to process batch") *>
                     ZIO.foreach_(batch.entries.map(_.done))(_.fail(e))
               }
             }
             .runDrain
             .toManaged_
             .fork
      _ <- (for {
               m          <- currentMetrics.getAndUpdate(_ => CurrentMetrics())
               now        <- instant
               meanLatency = if (m.latencies.nonEmpty)
                               Some(m.latencies.map(_.toMillis).sum.millis * (1.0 / m.latencies.length))
                             else None
               metrics     = ProducerMetrics(now, m.nrPublished, m.nrFailed, meanLatency)
               _          <- metricsCollector(metrics)
             } yield ()).delay(settings.metricsInterval).repeat(Schedule.fixed(settings.metricsInterval)).forkManaged
    } yield new Producer[T] {
      override def produce(r: ProducerRecord[T]): Task[ProduceResponse] =
        (for {
          now      <- instant
          request  <- makeProduceRequest(r, now)
          _        <- queue.offer(request)
          response <- request.done.await
        } yield response).provide(env)

      override def produceChunk(chunk: Chunk[ProducerRecord[T]]): Task[Seq[ProduceResponse]] =
        (for {
          now      <- instant
          requests <- ZIO.foreach(chunk)(makeProduceRequest(_, now))
          _        <- queue.offerAll(requests)
          results  <- ZIO.foreach(requests)(_.done.await)
        } yield results)
          .provide(env)

      private def makeProduceRequest(r: ProducerRecord[T], now: Instant) =
        for {
          done <- Promise.make[Throwable, ProduceResponse]
          data <- serializer.serialize(r.data)
          entry = PutRecordsRequestEntry
                    .builder()
                    .partitionKey(r.partitionKey)
                    .data(SdkBytes.fromByteBuffer(data))
                    .build()
        } yield ProduceRequest(entry, done, now)
    }

  val maxRecordsPerRequest     = 499             // This is a Kinesis API limitation // TODO because of fold issue, reduced by 1
  val maxPayloadSizePerRequest = 5 * 1024 * 1024 // 5 MB

  val recoverableErrorCodes = Set("ProvisionedThroughputExceededException", "InternalFailure", "ServiceUnavailable");

  final case class ProduceResponse(shardId: String, sequenceNumber: String)

  private final case class ProduceRequest(
    r: PutRecordsRequestEntry,
    done: Promise[Throwable, ProduceResponse],
    timestamp: Instant
  )

  private final case class PutRecordsBatch(entries: List[ProduceRequest], nrRecords: Int, payloadSize: Long) {
    def add(entry: ProduceRequest): PutRecordsBatch =
      copy(
        entries = entry +: entries,
        nrRecords = nrRecords + 1,
        payloadSize = payloadSize + entry.r.partitionKey().length + entry.r.data().asByteArray().length
      )

    lazy val entriesInOrder: Seq[ProduceRequest] = entries.reverse

    def isWithinLimits =
      nrRecords <= maxRecordsPerRequest &&
        payloadSize < maxPayloadSizePerRequest
  }

  private object PutRecordsBatch {
    val empty = PutRecordsBatch(List.empty, 0, 0)
  }

  private final def scheduleCatchRecoverable: Schedule[Any, Throwable, Throwable] =
    Schedule.recurWhile {
      case e: KinesisException if e.statusCode() / 100 != 4 => true
      case _: ReadTimeoutException                          => true
      case _: IOException                                   => true
      case _                                                => false
    }

  private final case class CurrentMetrics(
    nrPublished: Long = 0,
    nrFailed: Long = 0,
    latencies: Chunk[Duration] = Chunk.empty
  ) {
    def add(published: Long, failed: Long, latencies: Iterable[Duration]): CurrentMetrics =
      copy(
        nrPublished = nrPublished + published,
        nrFailed = nrFailed + failed,
        latencies = this.latencies ++ latencies
      )
  }

}
