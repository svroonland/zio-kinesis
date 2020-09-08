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
import software.amazon.awssdk.services.kinesis.model.{
  KinesisException,
  PutRecordsRequestEntry,
  PutRecordsResultEntry,
  Shard
}
import software.amazon.awssdk.utils.Md5Utils
import zio._
import zio.clock.{ instant, Clock }
import zio.duration._
import zio.logging.{ log, Logging }
import zio.stream.ZTransducer.Push
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
  inFlightCalls: Ref[Int],
  triggerUpdateShards: UIO[Unit]
) extends Producer[T] {
  val batcher: ZTransducer[Logging, Nothing, ProduceRequest, Seq[ProduceRequest]] =
    if (aggregate)
      foldWhileM(PutRecordsBatchAggregated.empty)(_.isWithinLimits) { (batch, record: ProduceRequest) =>
        shards.get.map(batch.add(record, _))
      }.mapM(_.toProduceRequests)
    else
      foldWhileM(PutRecordsBatch.empty)(_.isWithinLimits) { (batch, record: ProduceRequest) =>
        ZIO.succeed(batch.add(record))
      }.map(_.entriesInOrder)

  var runloop: ZIO[Logging with Clock, Nothing, Unit] =
    // Failed records get precedence
    (ZStream
      .tick(100.millis)
      .mapConcatChunkM(_ => failedQueue.takeBetween(1, Int.MaxValue).map(Chunk.fromIterable)) merge
      zStreamFromQueueWithMaxChunkSize(queue, maxChunkSize))
      .mapChunksM(chunk => log.trace(s"Dequeued chunk of size ${chunk.size}").as(chunk))
      // Batch records up to the Kinesis PutRecords request limits as long as downstream is busy
      .aggregateAsync(batcher)
      // Several putRecords requests in parallel
      .mapMParUnordered(settings.maxParallelRequests)(b => countInFlight(processBatch(b)))
      .runDrain

  private def processBatch(batch: Seq[ProduceRequest]) =
    (for {
      _ <- log.info(s"PutRecords for batch of size ${batch.map(_.aggregateCount).sum} (${batch.size} aggregated)")

      response           <- client
                    .putRecords(streamName, batch.map(_.r))
                    .tapError(e => log.warn(s"Error producing records, will retry if recoverable: $e"))
                    .retry(scheduleCatchRecoverable && settings.backoffRequests)
                    .timed
                    .tap(rt => log.debug(s"PutRecords took ${rt._1}")) andThen ZIO.second

      responseAndRequests = response.records().asScala.zip(batch)

      (newFailed, succeeded)    = if (response.failedRecordCount() > 0)
                                 responseAndRequests.partition {
                                   case (result, _) =>
                                     result.errorCode() != null && recoverableErrorCodes.contains(result.errorCode())
                                 }
                               else
                                 (Seq.empty, responseAndRequests)

      hasShardPredictionErrors <- checkShardPredictionErrors(responseAndRequests)
      _                        <- handleFailures(newFailed, repredict = hasShardPredictionErrors)
      _                        <- ZIO.foreach_(succeeded) {
             case (response, request) =>
               request.done.completeWith(
                 ZIO.succeed(ProduceResponse(response.shardId(), response.sequenceNumber(), request.attemptNumber))
               )
           }
      // TODO handle connection failure
    } yield ()).catchAll {
      case NonFatal(e) =>
        log.warn("Failed to process batch") *>
          ZIO.foreach_(batch.map(_.done))(_.fail(e))
    }.catchAllCause {
      case e: Cause[Throwable] =>
        log.error(s"oh noes!: ${e}") *>
          ZIO.foreach_(batch.map(_.done))(_.halt(e))
    }

  private def handleFailures(newFailed: collection.Seq[(PutRecordsResultEntry, ProduceRequest)], repredict: Boolean) = {
    val requests    = newFailed.map(_._2)
    val responses   = newFailed.map(_._1)
    val failedCount = requests.map(_.aggregateCount).sum

    for {
      _ <- currentMetrics.update(_.addFailures(failedCount))
      _ <- log.warn(s"Failed to produce ${failedCount} records").when(newFailed.nonEmpty)
      _ <- log.warn(responses.take(10).map(_.errorMessage()).mkString(",, ")).when(newFailed.nonEmpty)

      // The shard map may not yet be updated unless we're experiencing high latency
      updatedFailed <-
        if (repredict)
          shards.get.map(shardMap =>
            requests.map(r => r.newAttempt.copy(predictedShard = shardMap.shardForPutRecordsRequestEntry(r.r)))
          )
        else
          ZIO.succeed(requests.map(_.newAttempt))

      // TODO backoff for shard limit stuff
      _ <- failedQueue
             .offerAll(updatedFailed)
             .when(newFailed.nonEmpty) // TODO should be per shard
    } yield ()
  }

  private def checkShardPredictionErrors(responseAndRequests: Iterable[(PutRecordsResultEntry, ProduceRequest)]) = {
    val shardPredictionErrors = responseAndRequests.filter {
      case (result, request) => result.shardId() != request.predictedShard
    }

    val (succeeded, failed) = shardPredictionErrors.partition(_._1.errorCode() == null)

    ZIO
      .when(shardPredictionErrors.nonEmpty) {
        val maxProduceRequestTimestamp = shardPredictionErrors
          .map(_._2.timestamp.toEpochMilli)
          .max

        log
          .warn(
            s"${succeeded.map(_._2.aggregateCount).sum} records (aggregated as ${succeeded.size}) ended up " +
              s"on a different shard than expected and/or " +
              s"${failed.map(_._2.aggregateCount).sum} records (aggregated as ${failed.size}) would end up " +
              s"on a different shard than expected if they had succeeded. This may happen after a reshard."
          ) *> triggerUpdateShards.fork.whenM {
          shards
            .getAndUpdate(_.invalidate)
            .map(shardMap => !shardMap.invalid && shardMap.lastUpdated.toEpochMilli < maxProduceRequestTimestamp)
        }
      }
      .as(shardPredictionErrors.nonEmpty)
  }

  private def countInFlight[R0, E, A](e: ZIO[R0, E, A]): ZIO[R0 with Logging, E, A] =
    inFlightCalls
      .updateAndGet(_ + 1)
      .tap(inFlightCalls => log.debug(s"${inFlightCalls} PutRecords calls in flight"))
      .toManaged(_ =>
        inFlightCalls
          .updateAndGet(_ - 1)
          .tap(inFlightCalls => log.debug(s"${inFlightCalls} PutRecords calls in flight"))
      )
      .use_(e)

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
      shardMap <- shards.get
      now      <- instant
      request  <- makeProduceRequest(r, now, shardMap)
      _        <- queue.offer(request)
      response <- request.done.await
      finish   <- instant
      latency   = java.time.Duration.between(now, finish)
      _        <- currentMetrics.getAndUpdate(_.addSuccess(response.attempts, latency))
    } yield response).provide(env)

  override def produceChunk(chunk: Chunk[ProducerRecord[T]]): Task[Seq[ProduceResponse]] =
    (for {
      shardMap <- shards.get
      now      <- instant
      requests <- ZIO.foreach(chunk)(makeProduceRequest(_, now, shardMap))
      _        <- queue.offerAll(requests)
      results  <- ZIO.foreachParN(100)(requests)(_.done.await)
      finish   <- instant
      latency   = java.time.Duration.between(now, finish)
      latencies = results.map(_ => latency)
      _        <- currentMetrics.getAndUpdate(_.addSuccesses(results.map(_.attempts), latencies))
    } yield results)
      .provide(env)

  private def makeProduceRequest(r: ProducerRecord[T], now: Instant, shardMap: ShardMap) =
    for {
      done          <- Promise.make[Throwable, ProduceResponse]
      data          <- serializer.serialize(r.data)
      entry          = PutRecordsRequestEntry
                .builder()
                .partitionKey(r.partitionKey)
                .data(SdkBytes.fromByteBuffer(data))
                .build()
      predictedShard = shardMap.shardForPartitionKey(Option(entry.explicitHashKey()).getOrElse(entry.partitionKey()))
    } yield ProduceRequest(entry, done, now, predictedShard)
}

private[client] object ProducerLive {
  type ShardId      = String
  type PartitionKey = String

  val maxChunkSize: Int        = 512                        // Stream-internal max chunk size
  val maxRecordsPerRequest     = 500                        // This is a Kinesis API limitation
  val maxPayloadSizePerRequest = 5 * 1024 * 1024 - 2 * 1024 // 5 MB
  val maxPayloadSizePerRecord  = 1 * 1024 * 1024 - 2 * 1024 // 1 MB

  val recoverableErrorCodes = Set("ProvisionedThroughputExceededException", "InternalFailure", "ServiceUnavailable");

  final case class ProduceRequest(
    r: PutRecordsRequestEntry,
    done: Promise[Throwable, ProduceResponse],
    timestamp: Instant,
    predictedShard: ShardId,
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

      val records   = entries.zipWithIndex.map {
        case (e, index) => ProtobufAggregation.putRecordsRequestEntryToRecord(e.r, index)
      }
      val aggregate = builder
        .addAllRecords(records.asJava)
        .addAllExplicitHashKeyTable(
          entries.map(e => Option(e.r.explicitHashKey()).getOrElse("0")).asJava
        ) // TODO optimize: only filled ones
        .addAllPartitionKeyTable(entries.map(e => e.r.partitionKey()).asJava)
        .build()

//      println(
//        s"Aggregate size: ${aggregate.getSerializedSize}, predicted: ${payloadSize} (${aggregate.getSerializedSize * 100.0 / payloadSize} %)"
//      )
      aggregate
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
      } yield ProduceRequest(
        r,
        done,
        entries.head.timestamp,
        isAggregated = true,
        aggregateCount = entries.size,
        predictedShard = entries.head.predictedShard
      )
  }

  object PutRecordsAggregatedBatchForShard {
    private val empty: PutRecordsAggregatedBatchForShard = PutRecordsAggregatedBatchForShard(
      List.empty,
      ProtobufAggregation.magicBytes.length + ProtobufAggregation.checksumSize
    )

    def from(r: ProduceRequest): PutRecordsAggregatedBatchForShard =
      empty.add(r)
  }

  case class ShardMap(shards: Iterable[(ShardId, BigInt, BigInt)], lastUpdated: Instant, invalid: Boolean = false) {

    def shardForPutRecordsRequestEntry(e: PutRecordsRequestEntry): ShardId =
      shardForPartitionKey(Option(e.explicitHashKey()).getOrElse(e.partitionKey()))

    def shardForPartitionKey(key: PartitionKey): ShardId = {
      val hashBytes = Md5Utils.computeMD5Hash(key.getBytes(StandardCharsets.US_ASCII))
      val hashInt   = BigInt.apply(1, hashBytes)

      shards.collectFirst {
        case (shardId, minHashKey, maxHashKey) if hashInt >= minHashKey && hashInt <= maxHashKey => shardId
      }.getOrElse(throw new IllegalArgumentException(s"Could not find shard for partition key ${key}"))
    }

    def invalidate: ShardMap = copy(invalid = true)
  }

  object ShardMap {
    def fromShards(shards: Chunk[Shard], now: Instant): ShardMap =
      ShardMap(
        shards
          .map(s => (s.shardId(), BigInt(s.hashKeyRange().startingHashKey()), BigInt(s.hashKeyRange().endingHashKey())))
          .sortBy(_._2),
        now
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
        val shardId: ShardId = entry.predictedShard

        val shardBatch   = batches.get(shardId)
        val updatedBatch = shardBatch
          .map(_.add(entry))
          .getOrElse(PutRecordsAggregatedBatchForShard.from(entry))

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
    payloadSizeForEntry(entry) +
      3 +                                                                // Data
      3 + 2 +                                                            // Partition key
      Option(entry.explicitHashKey()).map(_.length + 2).getOrElse(1) + 3 // Explicit hash key

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

  /**
   * Like ZTransducer.foldM, but with 'while' instead of 'until' semantics regarding `contFn`
   */
  def foldWhileM[R, E, I, O](z: O)(contFn: O => Boolean)(f: (O, I) => ZIO[R, E, O]): ZTransducer[R, E, I, O] =
    ZTransducer {
      val initial = Some(z)

      def go(in: Chunk[I], state: O): ZIO[R, E, (Chunk[O], O)] =
        in.foldM[R, E, (Chunk[O], O)]((Chunk.empty, state)) {
          case ((os0, state), i) =>
            f(state, i).flatMap { o =>
              if (contFn(o))
                ZIO.succeed((os0, o))
              else
                f(z, i).map(zi => (os0 :+ state, zi))
            }
        }

      ZRef.makeManaged[Option[O]](initial).map { state =>
        {
          case Some(in) =>
            state.get.flatMap(s => go(in, s.getOrElse(z))).flatMap {
              case (os, s) =>
                state.set(Some(s)) *> Push.emit(os)
            }
          case None     =>
            state.getAndSet(None).map(_.fold[Chunk[O]](Chunk.empty)(Chunk.single(_)))
        }
      }
    }
}
