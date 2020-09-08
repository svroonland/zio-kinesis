package nl.vroste.zio.kinesis.client
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.time.Instant

import io.netty.handler.timeout.ReadTimeoutException
import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.Producer.ProduceResponse
import nl.vroste.zio.kinesis.client.ProducerLive._
import nl.vroste.zio.kinesis.client.ProducerMetrics.{ emptyAttempts, emptyLatency }
import nl.vroste.zio.kinesis.client.serde.Serializer
import nl.vroste.zio.kinesis.client.zionative.protobuf.Messages
import nl.vroste.zio.kinesis.client.zionative.protobuf.Messages.AggregatedRecord
import org.HdrHistogram.{ Histogram, IntCountsHistogram }
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.model.{ KinesisException, PutRecordsRequestEntry, Shard }
import software.amazon.awssdk.utils.Md5Utils
import zio._
import zio.clock.{ instant, Clock }
import zio.duration._
import zio.logging.{ log, Logging }
import zio.stream.{ ZStream, ZTransducer }

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

private[client] final class ProducerLive[R, R1, T](
  client: Client.Service,
  env: R with Clock,
  queue: Queue[ProduceRequest],
  failedQueue: Queue[ProduceRequest],
  serializer: Serializer[R, T],
  currentMetrics: Ref[CurrentMetrics],
  shards: Ref[ShardMap],
  settings: ProducerSettings,
  streamName: String,
  metricsCollector: ProducerMetrics => ZIO[R1, Nothing, Unit],
  aggregate: Boolean = false,
  inFlightCalls: Ref[Int]
) extends Producer[T] {
  val batcher: ZTransducer[Logging, Nothing, ProduceRequest, Seq[ProduceRequest]] =
    if (aggregate)
      ZTransducer
        .foldM(PutRecordsBatchAggregated.empty)(_.isWithinLimits) { (batch, record: ProduceRequest) =>
          shards.get.map(batch.add(record, _))
        }
        .mapM(_.toProduceRequests)
    else
      ZTransducer
        .fold(PutRecordsBatch.empty)(_.isWithinLimits)(_.add(_: ProduceRequest))
        .map(_.entriesInOrder)

  var runloop: ZIO[Logging with Clock, Nothing, Unit] =
    // Failed records get precedence
    (ZStream
      .tick(100.millis)
      .mapConcatChunkM(_ => failedQueue.takeBetween(1, Int.MaxValue).map(Chunk.fromIterable)) merge
      zStreamFromQueueWithMaxChunkSize(queue, settings.chunkSize))
      .mapChunksM(chunk => log.trace(s"Dequeued chunk of size ${chunk.size}").as(chunk))
      // Batch records up to the Kinesis PutRecords request limits as long as downstream is busy
      .aggregateAsync(batcher)
      // Several putRecords requests in parallel
      .mapMParUnordered(settings.maxParallelRequests)(b => countInFlight(processBatch(b)))
      .runDrain

  private def countInFlight[R1, E, A](e: ZIO[R1, E, A]): ZIO[R1 with Logging, E, A] =
    inFlightCalls
      .updateAndGet(_ + 1)
      .tap(inFlightCalls => log.debug(s"${inFlightCalls} PutRecords calls in flight"))
      .toManaged(_ =>
        inFlightCalls
          .updateAndGet(_ - 1)
          .tap(inFlightCalls => log.debug(s"${inFlightCalls} PutRecords calls in flight"))
      )
      .use_(e)

  private def processBatch(batch: Seq[ProduceRequest]) =
    (for {
      _ <- log.info(s"PutRecords for batch of size ${batch.map(_.aggregateCount).sum} (${batch.size} aggregated)")

      responseAndTiming <- client
                             .putRecords(streamName, batch.map(_.r))
                             .tapError(e => log.warn(s"Error producing records, will retry if recoverable: $e"))
                             .retry(scheduleCatchRecoverable && settings.backoffRequests)
                             .timed
      response           = responseAndTiming._2
      _                 <- log.debug(s"PutRecords took ${responseAndTiming._1}")

      maybeSucceeded = response.records().asScala.zip(batch)

      (newFailed, succeeded) = if (response.failedRecordCount() > 0)
                                 maybeSucceeded.partition {
                                   case (result, _) =>
                                     result.errorCode() != null && recoverableErrorCodes.contains(result.errorCode())
                                 }
                               else
                                 (Seq.empty, maybeSucceeded)

      // TODO invalidate shard map if predicted != actual

      failedCount            = newFailed.map(_._2.aggregateCount).sum
      _                     <- currentMetrics.update(_.addFailures(failedCount))
      _                     <- log.warn(s"Failed to produce ${failedCount} records").when(newFailed.nonEmpty)
      _                     <- log.warn(newFailed.take(10).map(_._1.errorMessage()).mkString(",, ")).when(newFailed.nonEmpty)

      // TODO backoff for shard limit stuff
      _ <- failedQueue
             .offerAll(newFailed.map(_._2.newAttempt))
             .when(newFailed.nonEmpty) // TODO should be per shard
      _ <- ZIO.foreach_(succeeded) {
             case (response, request) =>
               request.done.completeWith(
                 ZIO.succeed(ProduceResponse(response.shardId(), response.sequenceNumber(), request.attemptNumber))
               )
           }
    } yield ()).catchAll {
      case NonFatal(e) =>
        log.warn("Failed to process batch") *>
          ZIO.foreach_(batch.map(_.done))(_.fail(e))
    }

// Repeatedly produce metrics
  var metricsCollection: ZIO[R1 with Clock, Nothing, Long] = (for {
    now    <- instant
    m      <- currentMetrics.getAndUpdate(_ => CurrentMetrics.empty(now))
    metrics = ProducerMetrics(java.time.Duration.between(m.start, now), m.published, m.nrFailed, m.latency)
    _      <- metricsCollector(metrics)
  } yield ())
    .delay(settings.metricsInterval)
    .repeat(Schedule.spaced(settings.metricsInterval))

  override def produce(r: ProducerRecord[T]): Task[ProduceResponse] =
    (for {
      now      <- instant
      request  <- makeProduceRequest(r, now)
      _        <- queue.offer(request)
      response <- request.done.await
      finish   <- instant
      latency   = java.time.Duration.between(now, finish)
      _        <- currentMetrics.getAndUpdate(_.addSuccess(response.attempts, latency))
    } yield response).provide(env)

  override def produceChunk(chunk: Chunk[ProducerRecord[T]]): Task[Seq[ProduceResponse]] =
    (for {
      now      <- instant
      requests <- ZIO.foreach(chunk)(makeProduceRequest(_, now))
      _        <- queue.offerAll(requests)
      results  <- ZIO.foreachParN(100)(requests)(_.done.await)
      finish   <- instant
      latency   = java.time.Duration.between(now, finish)
      latencies = results.map(_ => latency)
      _        <- currentMetrics.getAndUpdate(_.addSuccesses(results.map(_.attempts), latencies))
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
  val maxRecordsPerRequest     = 499                          // This is a Kinesis API limitation // TODO because of fold issue, reduced by 1
  val maxPayloadSizePerRequest = 5 * 1024 * 1024 - 512 * 1024 // 5 MB
  val maxPayloadSizePerRecord  = 1 * 1024 * 1024 - 128 * 1024 // 1 MB

  val recoverableErrorCodes = Set("ProvisionedThroughputExceededException", "InternalFailure", "ServiceUnavailable");

  final case class ProduceRequest(
    r: PutRecordsRequestEntry,
    done: Promise[Throwable, ProduceResponse],
    timestamp: Instant,
    attemptNumber: Int = 1,
    isAggregated: Boolean = false,
    aggregateCount: Int = 1
  ) {
    def newAttempt = copy(attemptNumber = attemptNumber + 1)
  }

  final case class PutRecordsBatch(entries: List[ProduceRequest], nrRecords: Int, payloadSize: Long) {
    def add(entry: ProduceRequest): PutRecordsBatch =
      copy(
        entries = entry +: entries,
        nrRecords = nrRecords + 1,
        payloadSize = payloadSize + payloadSizeForEntry(entry.r)
      )

    lazy val entriesInOrder: Seq[ProduceRequest] = entries.reverse.sortBy(e => -1 * e.attemptNumber)

    def isWithinLimits =
      nrRecords <= maxRecordsPerRequest &&
        payloadSize < maxPayloadSizePerRequest
  }

  object PutRecordsBatch {
    val empty = PutRecordsBatch(List.empty, 0, 0)
  }

  final case class PutRecordsAggregatedBatchForShard(
    entries: List[ProduceRequest],
    payloadSize: Int
  ) {
    private def builtAggregate: AggregatedRecord = {
      val builder = Messages.AggregatedRecord.newBuilder()

      builder
        .addAllRecords(entries.zipWithIndex.map {
          case (e, index) => ProtobufAggregation.putRecordsRequestEntryToRecord(e.r, index)
        }.asJava)
        .addAllExplicitHashKeyTable(
          entries.map(e => Option(e.r.explicitHashKey()).getOrElse("0")).asJava
        ) // TODO optimize: only filled ones
        .addAllPartitionKeyTable(entries.map(e => e.r.partitionKey()).asJava)
        .build()
    }

    def add(r: ProduceRequest): PutRecordsAggregatedBatchForShard =
      copy(entries = r +: entries, payloadSize = payloadSize + payloadSizeForEntryAggregated(r.r))

    def isWithinLimits: Boolean =
      payloadSize <= maxPayloadSizePerRecord

    def toProduceRequest: UIO[ProduceRequest] =
      for {
        done <- Promise.make[Throwable, ProduceResponse]

        r  = PutRecordsRequestEntry
              .builder()
              .partitionKey(entries.head.r.partitionKey()) // First one?
              .data(SdkBytes.fromByteArray(ProtobufAggregation.encodeAggregatedRecord(builtAggregate).toArray))
              .build()
        _ <- ZIO.foreach_(entries)(e => e.done.completeWith(done.await))
      } yield ProduceRequest(r, done, entries.head.timestamp, 1, isAggregated = true, aggregateCount = entries.size)
  }

  object PutRecordsAggregatedBatchForShard {
    val empty: PutRecordsAggregatedBatchForShard = PutRecordsAggregatedBatchForShard(
      List.empty,
      ProtobufAggregation.magicBytes.size + ProtobufAggregation.checksumSize
    )
  }

  type ShardId      = String
  type PartitionKey = String

  case class ShardMap(shards: Iterable[(ShardId, BigInt, BigInt)]) {
    def shardForPartitionKey(key: PartitionKey): ShardId = {
      val hashBytes = Md5Utils.computeMD5Hash(key.getBytes(StandardCharsets.US_ASCII))
      val hashInt   = BigInt.apply(1, hashBytes)

      shards.collectFirst {
        case (shardId, minHashKey, maxHashKey) if hashInt >= minHashKey && hashInt <= maxHashKey => shardId
      }.getOrElse(throw new IllegalArgumentException(s"Could not find shard for partition key ${key}"))
    }

  }

  object ShardMap {
    def fromShards(shards: Chunk[Shard]): ShardMap =
      ShardMap(
        shards
          .map(s => (s.shardId(), BigInt(s.hashKeyRange().startingHashKey()), BigInt(s.hashKeyRange().endingHashKey())))
          .sortBy(_._2)
      )
  }

  // One batch per shard because the record limit of 1 MB is also the throughput limit for the shard (1 MB per second)
  final case class PutRecordsBatchAggregated(
    batches: Map[ShardId, PutRecordsAggregatedBatchForShard],
    retries: List[ProduceRequest],
    payloadSizeRetries: Int
  ) {
    val nrBatches = batches.size

    def payloadSize = payloadSizeRetries + batches.values.map(_.payloadSize).sum

    def add(entry: ProduceRequest, shardMap: ShardMap): PutRecordsBatchAggregated =
      if (entry.isAggregated)
        copy(retries = entry +: retries, payloadSizeRetries = payloadSizeRetries + payloadSizeForEntry(entry.r))
      else {
        val shardId: ShardId =
          shardMap.shardForPartitionKey(Option(entry.r.explicitHashKey()).getOrElse(entry.r.partitionKey()))

        // TODO can we somehow ensure that there's always at least one entry in a batch?
        val shardBatch = batches.getOrElse(shardId, PutRecordsAggregatedBatchForShard.empty)

        val updatedBatch = shardBatch.add(entry)

        // TODO this is an approximation
        copy(
          batches = batches + (shardId -> updatedBatch)
        )
      }

    def isWithinLimits: Boolean =
      (retries.length + nrBatches) <= maxRecordsPerRequest &&
        batches.values.forall(_.isWithinLimits) &&
        payloadSize < maxPayloadSizePerRequest

    def toProduceRequests: ZIO[Logging, Nothing, Seq[ProduceRequest]] =
      log.debug(
        s"Aggregated ${batches.values.map(_.entries.size).sum} records. " +
          s"Payload sizes: ${batches.values.map(_.payloadSize).mkString(", ")}, " +
          s"total size: ${batches.values.map(_.payloadSize).sum}"
      ) *>
        ZIO
          .foreach(batches.values.toSeq)(_.toProduceRequest)
          .map(_.reverse)
          .map(retries.reverse ++ _)
  }

  object PutRecordsBatchAggregated {
    def empty: PutRecordsBatchAggregated = PutRecordsBatchAggregated(Map.empty, List.empty, 0)
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

    def addSuccess(attempts: Int, latency1: Duration): CurrentMetrics = {
      val newLatency = latency.copy()
      newLatency.recordValue(latency1.toMillis max 0)

      val newPublished = published.copy()
      newPublished.recordValue(attempts.toLong)

      copy(published = newPublished, latency = newLatency)
    }

    def addFailures(nr: Int): CurrentMetrics =
      copy(nrFailed = nrFailed + nr)

    def addSuccesses(publishedWithAttempts: Seq[Int], latencies: Seq[Duration]): CurrentMetrics = {
      val newLatency = latency.copy()
      latencies.map(_.toMillis max 0).foreach(newLatency.recordValue)

      val newPublished = published.copy()
      publishedWithAttempts.map(_.toLong).foreach(newPublished.recordValue)

      copy(published = newPublished, latency = newLatency)
    }
  }

  object CurrentMetrics {
    def empty(now: Instant) =
      CurrentMetrics(start = now, published = emptyAttempts, nrFailed = 0, latency = emptyLatency)
  }

  def payloadSizeForEntry(entry: PutRecordsRequestEntry): Int =
    entry.partitionKey().length + entry.data().asByteArray().length

  def payloadSizeForEntryAggregated(entry: PutRecordsRequestEntry): Int =
    payloadSizeForEntry(entry) + 4 // To do find out overhead per record

  def zStreamFromQueueWithMaxChunkSize[R, E, O](
    queue: ZQueue[Nothing, R, Any, E, Nothing, O],
    chunkSize: Int = Int.MaxValue
  ): ZStream[R, E, O] =
    ZStream.repeatEffectChunkOption {
      queue
      // This is important for batching to work properly. Too large chunks means suboptimal usage of the max parallel requests
        .takeBetween(1, chunkSize)
        .map(Chunk.fromIterable)
        .catchAllCause(c =>
          queue.isShutdown.flatMap { down =>
            if (down && c.interrupted) IO.fail(None)
            else IO.halt(c).asSomeError
          }
        )
    }

}
