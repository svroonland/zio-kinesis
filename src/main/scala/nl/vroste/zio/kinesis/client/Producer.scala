package nl.vroste.zio.kinesis.client
import java.time.Instant

import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.Producer.ProduceResponse
import nl.vroste.zio.kinesis.client.serde.Serializer
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry
import zio._
import zio.clock.Clock
import zio.duration.{ Duration, _ }
import zio.stream.{ ZSink, ZStream }

import scala.collection.JavaConverters._

/**
 * Producer for Kinesis records
 *
 * Features:
 * - Batching of records into a single PutRecords calls to Kinesis for reduced IO overhead
 * - Retry requests with backoff on recoverable errors
 * - Retry individual records
 * - Rate limiting to respect shard capacity (TODO)
 *
 * Performs batching of records for efficiency, retry with back and retrying of failed put requests (eg due to Kinesis shard rate limits)
 *
 * Has an internal queue
 *
 * Inspired by https://docs.aws.amazon.com/streams/latest/dev/developing-producers-with-kpl.html
 *
 * Rate limits for PutRecords:
 * - 500 records per request
 * - Whole request max 5MB
 * - Each item max 1MB
 * - Each shard max 1000 records / s
 * - Each shard max 1MB / s
 */
trait Producer[T] {

  /**
   * Put a record on
   * Backpressures when too many requests are in flight
   *
   * @param r
   * @return
   */
  def produce(r: ProducerRecord[T]): ZIO[Clock, Throwable, ProduceResponse]

  /**
   * Backpressures when too many requests are in flight
   */
  def produceChunk(chunk: Chunk[ProducerRecord[T]]): ZIO[Clock, Throwable, List[ProduceResponse]]

  /**
   * ZSink for producing records
   */
  def sinkChunked: ZSink[Clock, Throwable, Nothing, Chunk[ProducerRecord[T]], Unit] =
    ZSink.drain.contramapM(produceChunk)
}

object Producer {
  def make[R, T](
    streamName: String,
    client: Client,
    serializer: Serializer[R, T],
    bufferSize: Int = 8192, // Prefer powers of 2
    maxBufferDuration: Duration = 500.millis,
    maxParallelRequests: Int = 24
  ): ZManaged[R with Clock, Throwable, Producer[T]] =
    for {
      env   <- ZIO.environment[R].toManaged_
      queue <- zio.Queue.bounded[ProduceRequest](bufferSize).toManaged(_.shutdown)

      failedQueue <- zio.Queue.bounded[ProduceRequest](bufferSize).toManaged(_.shutdown)

      // Failed records get precedence)
      _ <- (ZStream.fromQueue(failedQueue) merge ZStream.fromQueue(queue))
          // Buffer records up to maxBufferDuration or up to the Kinesis PutRecords request limit
            .aggregateAsyncWithin(
              foldWhileCondition[ProduceRequest, PutRecordsBatch](PutRecordsBatch.empty)(_.isWithinLimits)(_.add(_)),
              Schedule.spaced(maxBufferDuration)
            )
            // Several putRecords requests in parallel
            .mapMPar(maxParallelRequests) { batch: PutRecordsBatch =>
              for {
                response <- client
                             .putRecords(streamName, batch.entries.map(_.r))
                             // TODO retry on recoverable errors, eg service temporarily unavailable, but not on auth failure
                             .retry(Schedule.exponential(100.millis))

                (newFailed, succeeded) = response
                  .records()
                  .asScala
                  .zip(batch.entries.toList)
                  .partition { case (result, _) => result.errorCode() != null }
                // TODO backoff for shard limit stuff
                _ <- failedQueue.offerAll(newFailed.map(_._2)).delay(100.millis).fork // TODO should be per shard
                _ <- ZIO.traverse(succeeded) {
                      case (response, request) =>
                        request.done.succeed(ProduceResponse(response.shardId(), response.sequenceNumber()))
                    }
              } yield ()
            }
            .runDrain
            .toManaged_
            .fork
    } yield new Producer[T] {
      override def produce(r: ProducerRecord[T]): ZIO[Clock, Throwable, ProduceResponse] =
        for {
          now  <- zio.clock.currentDateTime
          done <- Promise.make[Throwable, ProduceResponse]
          data <- serializer.serialize(r.data).provide(env)
          entry = PutRecordsRequestEntry
            .builder()
            .partitionKey(r.partitionKey)
            .data(SdkBytes.fromByteBuffer(data))
            .build()
          request  = ProduceRequest(entry, done, now.toInstant)
          _        <- queue.offer(request)
          response <- done.await
        } yield response

      override def produceChunk(chunk: Chunk[ProducerRecord[T]]): ZIO[Clock, Throwable, List[ProduceResponse]] =
        zio.clock.currentDateTime.flatMap { now =>
          ZIO
            .traverse(chunk.toList) { r =>
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
        }.flatMap(requests => queue.offerAll(requests) *> ZIO.traverse(requests)(_.done.await))
    }

  val maxRecordsPerRequest     = 500             // This is a Kinesis API limitation
  val maxPayloadSizePerRequest = 5 * 1024 * 1024 // 5 MB

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
        payloadSize = payloadSize + entry.r.partitionKey().length + entry.r.data().asByteArray().size
      )

    def isWithinLimits =
      nrRecords <= maxRecordsPerRequest &&
        payloadSize < maxPayloadSizePerRequest
  }

  private object PutRecordsBatch {
    val empty = PutRecordsBatch(List.empty, 0, 0)
  }

  /**
   * Sink that aggregates while the aggregate meets the condition
   *
   * @param z Initial aggregate
   * @param condition Predicate to check on the aggregate to decide whether to include a new element
   * @param f Aggregation function
   * @tparam A Type of element
   * @tparam S Type of aggregate
   * @return
   */
  private final def foldWhileCondition[A, S](
    z: S
  )(condition: S => Boolean)(f: (S, A) => S): ZSink[Any, Nothing, A, A, S] =
    new ZSink[Any, Nothing, A, A, S] {
      type State = (S, Option[A])

      override def cont(state: (S, Option[A])): Boolean = state._2.isEmpty
      override def extract(state: (S, Option[A])): ZIO[Any, Nothing, (S, Chunk[A])] = {
        val (s, maybeLeftover) = state
        ZIO.succeed((s, maybeLeftover.map(Chunk.single).getOrElse(Chunk.empty)))
      }

      override def initial: ZIO[Any, Nothing, (S, Option[A])] = ZIO.succeed((z, None))

      override def step(state: (S, Option[A]), a: A): ZIO[Any, Nothing, (S, Option[A])] = ZIO.succeed {
        val (s, _)   = state
        val newState = f(s, a)
        if (condition(newState)) {
          (newState, None)
        } else {
          (s, Some(a))
        }
      }
    }
}
