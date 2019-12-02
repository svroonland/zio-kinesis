package nl.vroste.zio.kinesis.client
import java.time.Instant

import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.Producer.ProduceResponse
import nl.vroste.zio.kinesis.client.serde.Serializer
import zio._
import zio.clock.Clock
import zio.duration.{ Duration, _ }
import zio.stream.{ Sink, ZSink, ZStream }

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
  final case class ProduceResponse(shardId: String, sequenceNumber: String)

  private case class ProduceRequest[T](
    r: ProducerRecord[T],
    done: Promise[Throwable, ProduceResponse],
    timestamp: Instant
  )

  def make[R, T](
    streamName: String,
    client: Client,
    serializer: Serializer[R, T],
    bufferSize: Int = 1024, // Prefer powers of 2
    maxBufferDuration: Duration = 100.millis,
    maxParallelRequests: Int = 24
  ): ZManaged[R with Clock, Throwable, Producer[T]] =
    for {
      queue <- zio.Queue.bounded[ProduceRequest[T]](bufferSize).toManaged(_.shutdown)

      failedQueue <- zio.Queue.bounded[ProduceRequest[T]](bufferSize).toManaged(_.shutdown)

      // Failed records get precedence)
      _ <- (ZStream.fromQueue(failedQueue) merge ZStream.fromQueue(queue))
          // Buffer records up to maxBufferDuration or up to the Kinesis PutRecords request limit
            .aggregateAsyncWithin(
              Sink.collectAllN[ProduceRequest[T]](maxRecordsPerRequest.toLong),
              Schedule.spaced(maxBufferDuration)
            )
            // Several putRecords requests in parallel
            .mapMPar(maxParallelRequests) { requests =>
              for {
                response <- client
                             .putRecords(streamName, serializer, requests.map(_.r))
                             // TODO retry on recoverable errors, eg service temporarily unavailable, but not on auth failure
                             .retry(Schedule.exponential(1.second))

                (newFailed, succeeded) = response
                  .records()
                  .asScala
                  .zip(requests)
                  .partition { case (result, _) => result.errorCode() != null }
                _ = println(s"Success: ${succeeded.size}")
                _ = println(
                  s"Success: ${succeeded.size} (oldest: ${succeeded.headOption.map(_._2.timestamp)}), " +
                    s"failures: ${newFailed.size} (oldest ${newFailed.headOption.map(_._2.timestamp)})"
                )
                // TODO backoff for shard limit stuff
                _ <- failedQueue.offerAll(newFailed.map(_._2)).delay(1.second).fork // TODO should be per shard
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
          done     <- Promise.make[Throwable, ProduceResponse]
          now      <- zio.clock.currentDateTime
          request  = ProduceRequest(r, done, now.toInstant)
          _        <- queue.offer(request)
          response <- done.await
        } yield response

      override def produceChunk(chunk: Chunk[ProducerRecord[T]]): ZIO[Clock, Throwable, List[ProduceResponse]] =
        zio.clock.currentDateTime.flatMap { now =>
          ZIO
            .traverse(chunk.toVector) { r =>
              Promise.make[Throwable, ProduceResponse].map(ProduceRequest(r, _, now.toInstant))
            }
        }.flatMap(requests => queue.offerAll(requests) *> ZIO.traverse(requests)(_.done.await))
    }

  val maxRecordsPerRequest = 500 // This is a Kinesis API limitation
}
