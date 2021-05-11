package nl.vroste.zio.kinesis.client.producer

import io.github.vigoo.zioaws.kinesis.Kinesis
import io.github.vigoo.zioaws.kinesis.model.{
  PutRecordsRequest,
  PutRecordsRequestEntry,
  PutRecordsResponse,
  PutRecordsResultEntry
}
import io.netty.handler.timeout.ReadTimeoutException
import nl.vroste.zio.kinesis.client.Producer.ProduceResponse
import nl.vroste.zio.kinesis.client._
import nl.vroste.zio.kinesis.client.producer.ProducerLive.ProduceRequest
import nl.vroste.zio.kinesis.client.serde.Serializer
import software.amazon.awssdk.core.exception.SdkException
import software.amazon.awssdk.services.kinesis.model.KinesisException
import zio._
import zio.clock.{ instant, Clock }
import zio.duration._
import zio.logging.{ log, Logging }
import zio.stream.ZTransducer.Push
import zio.stream.{ ZStream, ZTransducer }

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.time.Instant
import scala.util.control.NonFatal

private[client] final class ProducerLive[R, R1, T](
  client: Kinesis.Service,
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
  triggerUpdateShards: UIO[Unit],
  throttler: ShardThrottler
) extends Producer[T] {
  import ProducerLive._
  import Util.ZStreamExtensions

  val runloop: ZIO[Logging with Clock, Nothing, Unit] = {
    val retries         = ZStream.fromQueue(failedQueue, maxChunkSize = maxChunkSize)
    val chunkBufferSize = Math.ceil(settings.bufferSize * 1.0 / maxChunkSize).toInt

    // Failed records get precedence
    (retries merge ZStream
      .fromQueue(queue, maxChunkSize)
      .mapChunksM(chunk => log.trace(s"Dequeued chunk of size ${chunk.size}").as(chunk))
      // Aggregate records per shard
      .groupByKey2(_.predictedShard, chunkBufferSize)
      .flatMapPar(Int.MaxValue, chunkBufferSize) {
        case (shardId @ _, requests) =>
          requests.aggregateAsync(if (aggregate) aggregator else ZTransducer.identity)
      })
      .groupByKey2(_.predictedShard, chunkBufferSize) // TODO can we avoid this second group by?
      .flatMapPar(Int.MaxValue, chunkBufferSize)(
        Function.tupled(throttleShardRequests)
      )
      // Batch records up to the Kinesis PutRecords request limits as long as downstream is busy
      .aggregateAsync(batcher)
      .filter(_.nonEmpty)                             // TODO why would this be necessary?
      // Several putRecords requests in parallel
      .flatMapPar(settings.maxParallelRequests, chunkBufferSize)(b =>
        ZStream.fromEffect(countInFlight(processBatch(b)))
      )
      .collect { case (Some(response), requests) => (response, requests) }
      .mapM((processBatchResponse _).tupled)
      .tap(metrics => currentMetrics.update(_ append metrics))
      .runDrain
  }

  private def throttleShardRequests(shardId: ShardId, requests: ZStream[Any, Nothing, ProduceRequest]) =
    ZStream.fromEffect(throttler.getForShard(shardId)).flatMap { throttlerForShard =>
      requests
        .mapChunks(
          _.map { request =>
            request
              .copy(complete =
                result => request.complete(result) *> result.zipLeft(throttlerForShard.addSuccess).ignore
              )
          }
        )
        .throttleShapeM(maxRecordsPerShardPerSecond.toLong, 1.second)(chunk =>
          throttlerForShard.throughputFactor.map(c => (chunk.size * 1.0 / c).toLong)
        )
        .throttleShapeM(maxIngestionPerShardPerSecond.toLong, 1.second)(chunk =>
          throttlerForShard.throughputFactor.map(c => (chunk.map(_.payloadSize).sum * 1.0 / c).toLong)
        )
    }

  private def processBatch(
    batch: Chunk[ProduceRequest]
  ): ZIO[Clock with Logging, Nothing, (Option[PutRecordsResponse.ReadOnly], Chunk[ProduceRequest])] = {
    val totalPayload = batch.map(_.data.length).sum
    (for {
      _        <- log.info(
             s"PutRecords for batch of size ${batch.map(_.aggregateCount).sum} (${batch.size} aggregated). " +
//               s"Payload sizes: ${batch.map(_.data.asByteArrayUnsafe().length).mkString(",")} " +
               s"(total = ${totalPayload} = ${totalPayload * 100.0 / maxPayloadSizePerRequest}%)."
           )

      // Avoid an allocation
      response <- client
                    .putRecords(new PutRecordsRequest(batch.map(_.asPutRecordsRequestEntry), streamName))
                    .mapError(_.toThrowable)
                    .tapError(e => log.warn(s"Error producing records, will retry if recoverable: $e"))
                    .retry(scheduleCatchRecoverable && settings.backoffRequests)
    } yield (Some(response), batch)).catchAll {
      case NonFatal(e) =>
        log.warn("Failed to process batch") *>
          ZIO.foreach_(batch)(_.complete(ZIO.fail(e))).as((None, batch))
    }
  }

  private def processBatchResponse(
    response: PutRecordsResponse.ReadOnly,
    batch: Chunk[ProduceRequest]
  ): ZIO[Logging with Clock, Nothing, CurrentMetrics] = {
    val totalPayload = batch.map(_.data.length).sum

    val responseAndRequests    = Chunk.fromIterable(response.recordsValue).zip(batch)
    val (newFailed, succeeded) =
      if (response.failedRecordCountValue.getOrElse(0) > 0)
        responseAndRequests.partition {
          case (result, _) =>
            result.errorCodeValue.exists(recoverableErrorCodes.contains)
        }
      else
        (Chunk.empty, responseAndRequests)
    for {

      now <- instant
      m1                             = CurrentMetrics.empty(now).addPayloadSize(totalPayload).addRecordSizes(batch.map(_.payloadSize))
      r                             <- checkShardPredictionErrors(responseAndRequests, m1)
      (hasShardPredictionErrors, m2) = r
      m3                            <- handleFailures(newFailed, repredict = hasShardPredictionErrors, m2)
      _                             <- ZIO.foreach_(succeeded) {
             case (response, request) =>
               request.complete(
                 ZIO.succeed(
                   ProduceResponse(
                     response.shardIdValue.get,
                     response.sequenceNumberValue.get,
                     request.attemptNumber,
                     completed = now
                   )
                 )
               )
           }
      // TODO handle connection failure
    } yield m3
  }

  private def handleFailures(
    newFailed: Chunk[(PutRecordsResultEntry.ReadOnly, ProduceRequest)],
    repredict: Boolean,
    metrics: CurrentMetrics
  ) = {
    val requests    = newFailed.map(_._2)
    val responses   = newFailed.map(_._1)
    val failedCount = requests.map(_.aggregateCount).sum

    for {
      _ <- ZIO.foreach_(requests)(r => throttler.addFailure(r.predictedShard))
      _ <- log.warn(s"Failed to produce ${failedCount} records").when(newFailed.nonEmpty)
      _ <- log.warn(responses.take(10).flatMap(_.errorCodeValue).mkString(", ")).when(newFailed.nonEmpty)

      // The shard map may not yet be updated unless we're experiencing high latency
      updatedFailed <-
        if (repredict)
          shards.get.map(shardMap =>
            requests.map(r => r.newAttempt.copy(predictedShard = shardMap.shardForPartitionKey(r.partitionKey)))
          )
        else
          ZIO.succeed(requests.map(_.newAttempt))

      // TODO backoff for shard limit stuff
      _ <- failedQueue
             .offerAll(updatedFailed)
             .when(newFailed.nonEmpty)
    } yield metrics.addFailures(failedCount)
  }

  private def checkShardPredictionErrors(
    responseAndRequests: Chunk[(PutRecordsResultEntry.ReadOnly, ProduceRequest)],
    metrics: CurrentMetrics
  ): ZIO[Logging, Nothing, (Boolean, CurrentMetrics)] = {
    val shardPredictionErrors = responseAndRequests.filter {
      case (result, request) => result.shardIdValue.exists(_ != request.predictedShard)
    }

    val (succeeded, failed) = shardPredictionErrors.partition(_._1.errorCodeValue.isEmpty)

    val updatedMetrics = metrics.addShardPredictionErrors(shardPredictionErrors.map(_._2.aggregateCount).sum.toLong)

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
      .as((shardPredictionErrors.nonEmpty, updatedMetrics))
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

  val collectMetrics: ZIO[R1 with Clock, Nothing, Unit] = for {
    now    <- instant
    m      <- currentMetrics.getAndUpdate(_ => CurrentMetrics.empty(now))
    metrics = ProducerMetrics(
                java.time.Duration.between(m.start, now),
                m.publishedHist,
                m.nrFailed,
                m.latencyHist,
                m.shardPredictionErrors,
                m.payloadSizeHist,
                m.recordSizeHist
              )
    _      <- metricsCollector(metrics)
  } yield ()

// Repeatedly produce metrics
  val metricsCollection: ZIO[R1 with Clock, Nothing, Long] = collectMetrics
    .delay(settings.metricsInterval)
    .repeat(Schedule.fixed(settings.metricsInterval))

  override def produce(r: ProducerRecord[T]): Task[ProduceResponse] =
    (for {
      shardMap        <- shards.get
      now             <- instant
      ar              <- makeProduceRequest(r, serializer, now, shardMap)
      (await, request) = ar
      _               <- queue.offer(request)
      response        <- await
      latency          = java.time.Duration.between(now, response.completed)
      _               <- currentMetrics.getAndUpdate(_.addSuccess(response.attempts, latency))
    } yield response).provide(env)

  override def produceChunk(chunk: Chunk[ProducerRecord[T]]): Task[Chunk[ProduceResponse]] =
    (for {
      shardMap <- shards.get
      now      <- instant

      done              <- Promise.make[Throwable, Chunk[ProduceResponse]]
      resultsCollection <- Ref.make[Chunk[ProduceResponse]](Chunk.empty)
      nrRequests         = chunk.size
      onDone             = (response: Task[ProduceResponse]) =>
                 response.flatMap { response =>
                   for {
                     responses <- resultsCollection.updateAndGet(_ :+ response)
                     _         <- ZIO.when(responses.size == nrRequests)(done.succeed(responses))
                   } yield ()
                 }.catchAll(done.fail).unit
      requests          <- ZIO.foreach(chunk) { r =>
                    for {
                      data          <- serializer.serialize(r.data)
                      predictedShard = shardMap.shardForPartitionKey(r.partitionKey)
                    } yield (done.await, ProduceRequest(data, r.partitionKey, onDone, now, predictedShard))
                  }
      _                 <- queue.offerAll(requests.map(_._2))
      results           <- done.await
      latencies          = results.map(r => java.time.Duration.between(now, r.completed))
      _                 <- currentMetrics.getAndUpdate(_.addSuccesses(results.map(_.attempts), latencies))
    } yield results)
      .provide(env)
}

private[client] object ProducerLive {
  type ShardId      = String
  type PartitionKey = String

  val maxChunkSize: Int        = 1024            // Stream-internal max chunk size
  val maxRecordsPerRequest     = 500             // This is a Kinesis API limitation
  val maxPayloadSizePerRequest = 5 * 1024 * 1024 // 5 MB
  val maxPayloadSizePerRecord  =
    1 * 1024 * 921 // 1 MB TODO actually 90%, to avoid underestimating and getting Kinesis errors
  val maxIngestionPerShardPerSecond = 1 * 1024 * 1024 // 1 MB
  val maxRecordsPerShardPerSecond   = 1000

  val recoverableErrorCodes = Set("ProvisionedThroughputExceededException", "InternalFailure", "ServiceUnavailable")

  final case class ProduceRequest(
    data: Chunk[Byte],
    partitionKey: String,
    complete: ZIO[Any, Throwable, ProduceResponse] => UIO[Unit],
    timestamp: Instant,
    predictedShard: ShardId,
    attemptNumber: Int = 1,
    isAggregated: Boolean = false,
    aggregateCount: Int = 1
  ) {
    def newAttempt = copy(attemptNumber = attemptNumber + 1)

    def isRetry: Boolean = attemptNumber > 1

    def payloadSize: Int = data.length + partitionKey.getBytes(StandardCharsets.UTF_8).length

    def asPutRecordsRequestEntry: PutRecordsRequestEntry = PutRecordsRequestEntry(data, partitionKey = partitionKey)
  }

  def makeProduceRequest[R, T](
    r: ProducerRecord[T],
    serializer: Serializer[R, T],
    now: Instant,
    shardMap: ShardMap
  ): ZIO[R, Throwable, (ZIO[Any, Throwable, ProduceResponse], ProduceRequest)] =
    for {
      done          <- Promise.make[Throwable, ProduceResponse]
      data          <- serializer.serialize(r.data)
      predictedShard = shardMap.shardForPartitionKey(r.partitionKey)
    } yield (done.await, ProduceRequest(data, r.partitionKey, done.completeWith(_).unit, now, predictedShard))

  final def scheduleCatchRecoverable: Schedule[Any, Throwable, Throwable] =
    Schedule.recurWhile(isRecoverableException)

  private def isRecoverableException(e: Throwable): Boolean =
    e match {
      case e: KinesisException if e.statusCode() / 100 != 4 => true
      case _: ReadTimeoutException                          => true
      case _: IOException                                   => true
      case e: SdkException if Option(e.getCause).isDefined  => isRecoverableException(e.getCause)
      case _                                                => false
    }

  def payloadSizeForEntry(entry: PutRecordsRequestEntry): Int =
    payloadSizeForEntry(entry.data, entry.partitionKey)

  def payloadSizeForEntry(data: Chunk[Byte], partitionKey: String): Int =
    partitionKey.getBytes(StandardCharsets.UTF_8).length + data.length

  def payloadSizeForEntryAggregated(entry: ProduceRequest): Int =
    payloadSizeForEntry(entry.data, entry.partitionKey) +
      3 +     // Data
      3 + 2 + // Partition key
      0       // entry.explicitHashKey.map(_.length + 2).getOrElse(1) + 3 // Explicit hash key

  /**
   * Like ZTransducer.foldM, but with 'while' instead of 'until' semantics regarding `contFn`
   */
  def foldWhile[R, E, I, O](z: O)(contFn: O => Boolean)(f: (O, I) => O): ZTransducer[R, E, I, O] =
    ZTransducer {
      val initial = Some(z)

      def go(in: Chunk[I], state: O): (Chunk[O], O) =
        in.foldLeft[(Chunk[O], O)]((Chunk.empty, state)) {
          case ((os0, state), i) =>
            val o = f(state, i)
            if (contFn(o))
              (os0, o)
            else
              (os0 :+ state, f(z, i))
        }

      ZRef.makeManaged[Option[O]](initial).map { state =>
        {
          case Some(in) =>
            state.get.map(s => go(in, s.getOrElse(z))).flatMap {
              case (os, s) =>
                state.set(Some(s)) *> Push.emit(os)
            }
          case None     =>
            state.getAndSet(None).map(_.fold[Chunk[O]](Chunk.empty)(Chunk.single(_)))
        }
      }
    }

  val batcher: ZTransducer[Any, Nothing, ProduceRequest, Chunk[ProduceRequest]] =
    foldWhile(PutRecordsBatch.empty)(_.isWithinLimits) { (batch, record: ProduceRequest) =>
      batch.add(record)
    }.map(_.entriesInOrder)

  val aggregator: ZTransducer[Any, Nothing, ProduceRequest, ProduceRequest] =
    foldWhile(PutRecordsAggregatedBatchForShard.empty)(_.isWithinLimits) { (batch, record: ProduceRequest) =>
      batch.add(record)
    }.mapM(_.toProduceRequest)
}
