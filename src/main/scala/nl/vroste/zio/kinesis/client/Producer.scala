package nl.vroste.zio.kinesis.client
import java.io.IOException

import io.github.vigoo.zioaws.kinesis
import io.github.vigoo.zioaws.kinesis.Kinesis
import io.github.vigoo.zioaws.kinesis.model.{ PutRecordsRequest, PutRecordsRequestEntry }
import io.netty.handler.timeout.ReadTimeoutException
import nl.vroste.zio.kinesis.client.Producer.ProduceResponse
import nl.vroste.zio.kinesis.client.serde.Serializer
import software.amazon.awssdk.services.kinesis.model.KinesisException
import zio._
import zio.clock.Clock
import zio.duration.{ Duration, _ }
import zio.logging._
import zio.stream.{ ZSink, ZStream, ZTransducer }

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
 * Records are batched for up to `maxBufferDuration` time, or 500 records or 5MB of payload size,
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
  bufferSize: Int = 8192, // Prefer powers of 2
  maxBufferDuration: Duration = 500.millis,
  maxParallelRequests: Int = 24,
  backoffRequests: Schedule[Clock, Throwable, Any] = Schedule.exponential(500.millis) && Schedule.recurs(5),
  failedDelay: Duration = 100.millis
)

// TODO add logging
object Producer {
  def make[R, T](
    streamName: String,
    serializer: Serializer[R, T],
    settings: ProducerSettings = ProducerSettings()
  ): ZManaged[R with Clock with Kinesis with Logging, Throwable, Producer[T]] =
    for {
      env   <- ZIO.environment[R with Clock].toManaged_
      queue <- zio.Queue.bounded[ProduceRequest](settings.bufferSize).toManaged(_.shutdown)

      failedQueue <- zio.Queue.bounded[ProduceRequest](settings.bufferSize).toManaged(_.shutdown)

      // Failed records get precedence)
      _ <- (ZStream.fromQueue(failedQueue) merge ZStream.fromQueue(queue))
           // Buffer records up to maxBufferDuration or up to the Kinesis PutRecords request limit
             .aggregateAsyncWithin(
               ZTransducer.fold(PutRecordsBatch.empty)(_.isWithinLimits)(_.add(_)),
               Schedule.duration(settings.maxBufferDuration)
             )
             // Several putRecords requests in parallel
             .mapMParUnordered(settings.maxParallelRequests) { batch: PutRecordsBatch =>
               (for {
                 response              <- kinesis
                               .putRecords(PutRecordsRequest(batch.entriesInOrder.map(_.r), streamName))
                               .mapError(_.toThrowable)
                               .tapError(e => log.warn(s"Error producing records, will retry if recoverable: $e"))
                               .retry(scheduleCatchRecoverable && settings.backoffRequests)

                 maybeSucceeded         = response.recordsValue
                                    .zip(batch.entriesInOrder)
                 (newFailed, succeeded) = if (response.failedRecordCountValue.getOrElse(0) > 0)
                                            maybeSucceeded.partition {
                                              case (result, _) =>
                                                result.errorCodeValue.exists(recoverableErrorCodes.contains)
                                            }
                                          else
                                            (Seq.empty, maybeSucceeded)

                 _                     <- log.info(s"Failed to produce ${newFailed.size} records").when(newFailed.nonEmpty)
                 // TODO backoff for shard limit stuff
                 _                     <- failedQueue
                        .offerAll(newFailed.map(_._2))
                        .delay(settings.failedDelay)
                        .fork
                        .when(newFailed.nonEmpty) // TODO should be per shard
                 _                     <- ZIO.foreach_(succeeded) {
                        case (response, request) =>
                          request.done.succeed(
                            ProduceResponse(response.shardIdValue.get, response.sequenceNumberValue.get)
                          )
                      }
               } yield ()).catchAll { case NonFatal(e) => ZIO.foreach_(batch.entries.map(_.done))(_.fail(e)) }
             }
             .runDrain
             .toManaged_
             .fork
    } yield new Producer[T] {
      override def produce(r: ProducerRecord[T]): Task[ProduceResponse] =
        (for {
          request  <- makeProduceRequest(r)
          _        <- queue.offer(request)
          response <- request.done.await
        } yield response).provide(env)

      override def produceChunk(chunk: Chunk[ProducerRecord[T]]): Task[Seq[ProduceResponse]] =
        (for {
          requests <- ZIO.foreach(chunk)(makeProduceRequest)
          _        <- queue.offerAll(requests)
          results  <- ZIO.foreach(requests)(_.done.await)
        } yield results)
          .provide(env)

      private def makeProduceRequest(r: ProducerRecord[T]) =
        for {
          done <- Promise.make[Throwable, ProduceResponse]
          data <- serializer.serialize(r.data)
          entry = PutRecordsRequestEntry(Chunk.fromByteBuffer(data), partitionKey = r.partitionKey)
        } yield ProduceRequest(entry, done)
    }

  val maxRecordsPerRequest     = 499             // This is a Kinesis API limitation // TODO because of fold issue, reduced by 1
  val maxPayloadSizePerRequest = 5 * 1024 * 1024 // 5 MB

  val recoverableErrorCodes = Set("ProvisionedThroughputExceededException", "InternalFailure", "ServiceUnavailable");

  final case class ProduceResponse(shardId: String, sequenceNumber: String)

  private final case class ProduceRequest(r: PutRecordsRequestEntry, done: Promise[Throwable, ProduceResponse])

  private final case class PutRecordsBatch(entries: List[ProduceRequest], nrRecords: Int, payloadSize: Long) {
    def add(entry: ProduceRequest): PutRecordsBatch =
      copy(
        entries = entry +: entries,
        nrRecords = nrRecords + 1,
        payloadSize = payloadSize + entry.r.partitionKey.length + entry.r.data.length
      )

    lazy val entriesInOrder: List[ProduceRequest] = entries.reverse

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
}
