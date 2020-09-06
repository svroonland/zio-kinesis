package nl.vroste.zio.kinesis.client
import java.io.IOException
import java.time.Instant

import com.google.protobuf.ByteString
import io.netty.handler.timeout.ReadTimeoutException
import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.Producer.ProduceResponse
import nl.vroste.zio.kinesis.client.ProducerLive._
import nl.vroste.zio.kinesis.client.ProducerMetrics.{ emptyAttempts, emptyLatency }
import nl.vroste.zio.kinesis.client.serde.Serializer
import nl.vroste.zio.kinesis.client.zionative.protobuf.Messages
import nl.vroste.zio.kinesis.client.zionative.protobuf.Messages.AggregatedRecord
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.model.{ KinesisException, PutRecordsRequestEntry }
import org.HdrHistogram.{ Histogram, IntCountsHistogram }
import zio.clock.{ instant, Clock }
import zio.logging.{ log, Logging }
import zio.stream.{ ZStream, ZTransducer }
import zio.duration._
import zio._

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

private[client] final class ProducerLive[R, R1, T](
  client: Client.Service,
  env: R with Clock,
  queue: Queue[ProduceRequest],
  failedQueue: Queue[ProduceRequest],
  serializer: Serializer[R, T],
  currentMetrics: Ref[CurrentMetrics],
  settings: ProducerSettings,
  streamName: String,
  metricsCollector: ProducerMetrics => ZIO[R1, Nothing, Unit],
  aggregate: Boolean = false
) extends Producer[T] {
  var runloop: ZIO[Logging with Clock, Nothing, Unit] =
    // Failed records get precedence)
    (ZStream
      .tick(100.millis)
      .mapConcatChunkM(_ => failedQueue.takeBetween(1, Int.MaxValue).map(Chunk.fromIterable)) merge ZStream
      .fromQueue(queue))
    // Batch records up to the Kinesis PutRecords request limits as long as downstream is busy
      .aggregateAsync(ZTransducer.fold(PutRecordsBatch.empty)(_.isWithinLimits)(_.add(_)))
      // Several putRecords requests in parallel
      .mapMParUnordered(settings.maxParallelRequests)(processBatch)
      .runDrain

  private def processBatch(batch: PutRecordsBatch) =
    (for {
      _ <- log.info(s"Producing batch of size ${batch.nrRecords}")

      response      <- client
                    .putRecords(streamName, batch.entriesInOrder.map(_.r))
                    .tapError(e => log.warn(s"Error producing records, will retry if recoverable: $e"))
                    .retry(scheduleCatchRecoverable && settings.backoffRequests)

      maybeSucceeded = response.records().asScala.zip(batch.entriesInOrder)

      (newFailed, succeeded) = if (response.failedRecordCount() > 0)
                                 maybeSucceeded.partition {
                                   case (result, _) =>
                                     result.errorCode() != null && recoverableErrorCodes.contains(result.errorCode())
                                 }
                               else
                                 (Seq.empty, maybeSucceeded)

      // Collect metrics
      now                   <- instant
      _                     <- currentMetrics.update(
             _.add(
               succeeded.map(_._2.attemptNumber).toSeq,
               newFailed.size.toLong,
               succeeded
                 .map(_._2.timestamp)
                 .map(timestamp => java.time.Duration.between(timestamp, now))
             )
           )
      _                     <- log.info(s"Failed to produce ${newFailed.size} records").when(newFailed.nonEmpty)
      // TODO backoff for shard limit stuff
      _                     <- failedQueue
             .offerAll(newFailed.map(_._2.newAttempt))
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

// Repeatedly produce metrics
  var metricsCollection: ZIO[R1 with Clock, Nothing, Long] = (for {
    now    <- instant
    m      <- currentMetrics.getAndUpdate(_ => CurrentMetrics.empty(now))
    metrics = ProducerMetrics(java.time.Duration.between(m.start, now), m.published, m.nrFailed, m.latency)
    _      <- metricsCollector(metrics)
  } yield ())
    .delay(settings.metricsInterval)
    .repeat(Schedule.fixed(settings.metricsInterval))

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

private[client] object ProducerLive {
  val maxRecordsPerRequest     = 499             // This is a Kinesis API limitation // TODO because of fold issue, reduced by 1
  val maxPayloadSizePerRequest = 5 * 1024 * 1024 // 5 MB

  val recoverableErrorCodes = Set("ProvisionedThroughputExceededException", "InternalFailure", "ServiceUnavailable");

  final case class ProduceRequest(
    r: PutRecordsRequestEntry,
    done: Promise[Throwable, ProduceResponse],
    timestamp: Instant,
    attemptNumber: Int = 1
  ) {
    def newAttempt = copy(attemptNumber = attemptNumber + 1)
  }

  final case class PutRecordsBatch(entries: List[ProduceRequest], nrRecords: Int, payloadSize: Long) {
    def add(entry: ProduceRequest): PutRecordsBatch =
      copy(
        entries = entry +: entries,
        nrRecords = nrRecords + 1,
        payloadSize = payloadSize + entry.r.partitionKey().length + entry.r.data().asByteArray().length
      )

    lazy val entriesInOrder: Seq[ProduceRequest] = entries.reverse.sortBy(e => -1 * e.attemptNumber)

    def isWithinLimits =
      nrRecords <= maxRecordsPerRequest &&
        payloadSize < maxPayloadSizePerRequest
  }

  object PutRecordsBatch {
    val empty = PutRecordsBatch(List.empty, 0, 0)
  }

  final def scheduleCatchRecoverable: Schedule[Any, Throwable, Throwable] =
    Schedule.recurWhile {
      case e: KinesisException if e.statusCode() / 100 != 4 => true
      case _: ReadTimeoutException                          => true
      case _: IOException                                   => true
      case _                                                => false
    }

  final case class CurrentMetrics(
    start: Instant,
    published: IntCountsHistogram, // Tracks number of attempts
    nrFailed: Long,
    latency: Histogram
  ) {
    val nrPublished = published.getTotalCount

    def add(publishedWithAttempts: Seq[Int], failed: Long, latencies: Iterable[Duration]): CurrentMetrics = {
      val newLatency = latency.copy()
      latencies.map(_.toMillis).foreach(newLatency.recordValue)

      val newPublished = published.copy()
      publishedWithAttempts.map(_.toLong).foreach(newPublished.recordValue)

      copy(
        published = newPublished,
        nrFailed = nrFailed + failed,
        latency = newLatency
      )
    }
  }

  object CurrentMetrics {
    def empty(now: Instant) =
      CurrentMetrics(start = now, published = emptyAttempts, nrFailed = 0, latency = emptyLatency)
  }

  def recordsToAggregatedRecord(entries: Seq[PutRecordsRequestEntry]): Messages.AggregatedRecord =
    AggregatedRecord
      .newBuilder()
      .addAllRecords(entries.zipWithIndex.map((recordToRecord _).tupled).asJavaCollection)
      .addAllPartitionKeyTable(entries.map(_.partitionKey()).asJavaCollection)
      .addAllExplicitHashKeyTable(entries.map(_.explicitHashKey()).asJavaCollection)
      .build()

  def recordToRecord(r: PutRecordsRequestEntry, tableIndex: Int): Messages.Record =
    Messages.Record
      .newBuilder()
      .setData(ByteString.copyFrom(r.data().asByteArrayUnsafe()))
      .setPartitionKeyIndex(tableIndex)
      .setExplicitHashKeyIndex(tableIndex)
      .build()
}
