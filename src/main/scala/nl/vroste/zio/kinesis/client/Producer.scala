package nl.vroste.zio.kinesis.client
import java.time.Instant

import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.Producer.ProduceResponse
import nl.vroste.zio.kinesis.client.serde.Serializer
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.model.{ KinesisException, PutRecordsRequestEntry }
import zio._
import zio.clock.Clock
import zio.duration.{ Duration, _ }
import zio.stream.{ ZSink, ZStream, ZTransducer }

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import scala.collection.immutable.Nil

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
  def produceChunk(chunk: Chunk[ProducerRecord[T]]): Task[List[ProduceResponse]]

  /**
   * ZSink interface to the Producer
   */
  def sinkChunked: ZSink[Any, Throwable, Chunk[ProducerRecord[T]], Unit] =
    ZSink.drain.contramapM(produceChunk)
}

final case class ProducerSettings(
  bufferSize: Int = 8192, // Prefer powers of 2
  maxBufferDuration: Duration = 500.millis,
  maxParallelRequests: Int = 24,
  backoffRequests: Schedule[Clock, Throwable, Any] = Schedule.exponential(500.millis) && Schedule.recurs(5),
  failedDelay: Duration = 100.millis
)

object Producer {
  def make[R, T](
    streamName: String,
    client: Client,
    serializer: Serializer[R, T],
    settings: ProducerSettings = ProducerSettings()
  ): ZManaged[R with Clock, Throwable, Producer[T]] =
    for {
      env   <- ZIO.environment[R with Clock].toManaged_
      queue <- zio.Queue.bounded[ProduceRequest](settings.bufferSize).toManaged(_.shutdown)

      failedQueue <- zio.Queue.bounded[ProduceRequest](settings.bufferSize).toManaged(_.shutdown)

      // Failed records get precedence)
      _ <- (ZStream.fromQueue(failedQueue) merge ZStream.fromQueue(queue))
           // Buffer records up to maxBufferDuration or up to the Kinesis PutRecords request limit
             .aggregateAsyncWithin(
               ZTransducer.fold(PutRecordsBatch.empty)(_.isWithinLimits)(_.add(_)),
               Schedule.spaced(settings.maxBufferDuration)
             )
             .map(_.removeLastAdded) // Because ZTransducer.fold will include the entry that was just over the limit
             // Several putRecords requests in parallel
             .mapMPar(settings.maxParallelRequests) { batch: PutRecordsBatch =>
               println(s"PRODUCING BATCH OF SIZE ${batch.entries.size}")
               (for {
                 response              <- client
                               .putRecords(streamName, batch.entries.map(_.r))
                               .retry(scheduleCatchRecoverable && settings.backoffRequests)

                 maybeSucceeded         = response
                                    .records()
                                    .asScala
                                    .zip(batch.entries)
                 (newFailed, succeeded) = if (response.failedRecordCount() > 0)
                                            maybeSucceeded.partition {
                                              case (result, _) =>
                                                result.errorCode() != null && recoverableErrorCodes.contains(
                                                  result.errorCode()
                                                )
                                            }
                                          else
                                            (Seq.empty, maybeSucceeded)

                 // TODO backoff for shard limit stuff
                 _                     <- failedQueue
                        .offerAll(newFailed.map(_._2))
                        .delay(settings.failedDelay)
                        .fork // TODO should be per shard
                 _                     <- ZIO.foreach(succeeded) {
                        case (response, request) =>
                          request.done.succeed(ProduceResponse(response.shardId(), response.sequenceNumber()))
                      }
               } yield ()).catchAll { case NonFatal(e) => ZIO.foreach_(batch.entries.map(_.done))(_.fail(e)) }
             }
             .runDrain
             .toManaged_
             .fork
    } yield new Producer[T] {
      override def produce(r: ProducerRecord[T]): Task[ProduceResponse] =
        for {
          now      <- zio.clock.currentDateTime.provide(env)
          done     <- Promise.make[Throwable, ProduceResponse]
          data     <- serializer.serialize(r.data).provide(env)
          entry     = PutRecordsRequestEntry
                    .builder()
                    .partitionKey(r.partitionKey)
                    .data(SdkBytes.fromByteBuffer(data))
                    .build()
          request   = ProduceRequest(entry, done, now.toInstant)
          _        <- queue.offer(request)
          response <- done.await
        } yield response

      override def produceChunk(chunk: Chunk[ProducerRecord[T]]): Task[List[ProduceResponse]] =
        zio.clock.currentDateTime
          .provide(env)
          .flatMap { now =>
            ZIO
              .foreach(chunk.toList) { r =>
                for {
                  done <- Promise.make[Throwable, ProduceResponse]
                  data <- serializer.serialize(r.data).provide(env)
                  entry = PutRecordsRequestEntry
                            .builder()
                            .partitionKey(r.partitionKey)
                            .data(SdkBytes.fromByteBuffer(data))
                            .build()
                } yield ProduceRequest(entry, done, now.toInstant)
              }
          }
          .flatMap(requests => queue.offerAll(requests) *> ZIO.foreachPar(requests)(_.done.await))
    }

  val maxRecordsPerRequest     = 500             // This is a Kinesis API limitation
  val maxPayloadSizePerRequest = 5 * 1024 * 1024 // 5 MB

  val recoverableErrorCodes = Set("ProvisionedThroughputExceededException", "InternalFailure", "ServiceUnavailable");

  final case class ProduceResponse(shardId: String, sequenceNumber: String)

  private final case class ProduceRequest(
    r: PutRecordsRequestEntry,
    done: Promise[Throwable, ProduceResponse],
    timestamp: Instant
  )

  private final case class PutRecordsBatch(entries: List[ProduceRequest], nrRecords: Int, payloadSize: Long) {
    import PutRecordsBatch.payloadSizeForEntry
    def add(entry: ProduceRequest): PutRecordsBatch =
      copy(
        entries = entry +: entries,
        nrRecords = nrRecords + 1,
        payloadSize = payloadSize + payloadSizeForEntry(entry)
      )

    def isWithinLimits =
      nrRecords <= maxRecordsPerRequest &&
        payloadSize <= maxPayloadSizePerRequest

    def removeLastAdded: PutRecordsBatch =
      entries match {
        case head :: tail =>
          copy(
            entries = tail,
            nrRecords = nrRecords - 1,
            payloadSize = payloadSize - payloadSizeForEntry(head)
          )
        case Nil          => this
      }
  }

  private object PutRecordsBatch {
    val empty = PutRecordsBatch(List.empty, 0, 0)

    def payloadSizeForEntry(entry: ProduceRequest) =
      entry.r.partitionKey().length + entry.r.data().asByteArray().length
  }

  private final def scheduleCatchRecoverable: Schedule[Any, Throwable, Throwable] =
    Schedule.doWhile {
      case e: KinesisException if e.statusCode() / 100 != 4 => true
      case _                                                => false
    }
}
