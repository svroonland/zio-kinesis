package nl.vroste.zio.kinesis.client.producer

import java.io.IOException
import java.time.Instant

import io.netty.handler.timeout.ReadTimeoutException
import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.Producer.ProduceResponse
import nl.vroste.zio.kinesis.client._
import nl.vroste.zio.kinesis.client.producer.ProducerLive.ProduceRequest
import nl.vroste.zio.kinesis.client.serde.Serializer
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.model.{ KinesisException, PutRecordsRequestEntry, PutRecordsResultEntry }
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
  triggerUpdateShards: UIO[Unit],
  throttler: ShardThrottler
) extends Producer[T] {
  import ProducerLive._
  import Util.ZStreamExtensions

  val runloop: ZIO[Logging with Clock, Nothing, Unit] = {
    val retries = ZStream.fromQueue(failedQueue, maxChunkSize)

    // Failed records get precedence
    (retries merge ZStream
      .fromQueue(queue, maxChunkSize)
      .mapChunksM(chunk => log.trace(s"Dequeued chunk of size ${chunk.size}").as(chunk))
      // Aggregate records per shard
      .groupByKey2(_.predictedShard)
      .flatMapPar(Int.MaxValue, 1) {
        case (shardId @ _, requests) =>
          requests.aggregateAsync(if (aggregate) aggregator else ZTransducer.identity)
      })
      .groupByKey2(_.predictedShard) // TODO can we avoid this second group by?
      .flatMapPar(Int.MaxValue, 1)(Function.tupled(throttleShardRequests))
      // Batch records up to the Kinesis PutRecords request limits as long as downstream is busy
      .aggregateAsync(batcher)
      .filter(_.nonEmpty)            // TODO why would this be necessary?
      // Several putRecords requests in parallel
      .mapMParUnordered(settings.maxParallelRequests)(b => countInFlight(processBatch(b)))
      .runDrain
  }

  private def throttleShardRequests(shardId: ShardId, requests: ZStream[Any, Nothing, ProduceRequest]) =
    requests
      .mapChunks(
        _.map { request =>
          request
            .copy(complete =
              result => request.complete(result) *> result.tap(_ => throttler.addSuccess(shardId)).ignore
            )
        }
      )
      .throttleShapeM(maxRecordsPerShardPerSecond.toLong, 1.second)(chunk =>
        throttler.throughputFactor(shardId).map(c => (chunk.size * 1.0 / c).toLong)
      )
      .throttleShapeM(maxIngestionPerShardPerSecond.toLong, 1.second)(chunk =>
        throttler.throughputFactor(shardId).map(c => (chunk.map(_.payloadSize).sum * 1.0 / c).toLong)
      )

  private def processBatch(batch: Seq[ProduceRequest]): ZIO[Clock with Logging, Nothing, Unit] = {
    val totalPayload = batch.map(_.r.data().asByteArrayUnsafe().length).sum
    (for {
      _                  <- log.info(
             s"PutRecords for batch of size ${batch.map(_.aggregateCount).sum} (${batch.size} aggregated). " +
//               s"Payload sizes: ${batch.map(_.r.data().asByteArrayUnsafe().length).mkString(",")} " +
               s"(total = ${totalPayload} = ${totalPayload * 100.0 / maxPayloadSizePerRequest}%)."
           )

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
      now                      <- instant
      _                        <- currentMetrics.update(_.addPayloadSize(totalPayload).addRecordSizes(batch.map(_.payloadSize)))
      _                        <- ZIO.foreach_(succeeded) {
             case (response, request) =>
               request.complete(
                 ZIO.succeed(
                   ProduceResponse(
                     response.shardId(),
                     response.sequenceNumber(),
                     request.attemptNumber,
                     completed = now
                   )
                 )
               )
           }
      // TODO handle connection failure
    } yield ()).catchAll {
      case NonFatal(e) =>
        log.warn("Failed to process batch") *>
          ZIO.foreach_(batch)(_.complete(ZIO.fail(e)))
    }
  }

  private def handleFailures(newFailed: collection.Seq[(PutRecordsResultEntry, ProduceRequest)], repredict: Boolean) = {
    val requests    = newFailed.map(_._2)
    val responses   = newFailed.map(_._1)
    val failedCount = requests.map(_.aggregateCount).sum

    for {
      _ <- currentMetrics.update(_.addFailures(failedCount))
      _ <- ZIO.foreach_(requests)(r => throttler.addFailure(r.predictedShard))
      _ <- log.warn(s"Failed to produce ${failedCount} records").when(newFailed.nonEmpty)
      _ <- log.warn(responses.take(10).map(_.errorCode()).mkString(", ")).when(newFailed.nonEmpty)

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
             .when(newFailed.nonEmpty)
    } yield ()
  }

  private def checkShardPredictionErrors(responseAndRequests: Iterable[(PutRecordsResultEntry, ProduceRequest)]) = {
    val shardPredictionErrors = responseAndRequests.filter {
      case (result, request) => Option(result.shardId()).exists(_ != request.predictedShard)
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
        } *> currentMetrics.update(
          _.addShardPredictionErrors(shardPredictionErrors.map(_._2.aggregateCount).sum.toLong)
        )
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

  val collectMetrics: ZIO[R1 with Clock, Nothing, Unit] = for {
    now    <- instant
    m      <- currentMetrics.getAndUpdate(_ => CurrentMetrics.empty(now))
    metrics = ProducerMetrics(
                java.time.Duration.between(m.start, now),
                m.published,
                m.nrFailed,
                m.latency,
                m.shardPredictionErrors,
                m.payloadSize,
                m.recordSize
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

  override def produceChunk(chunk: Chunk[ProducerRecord[T]]): Task[Seq[ProduceResponse]] =
    (for {
      shardMap <- shards.get
      now      <- instant
      requests <- ZIO.foreach(chunk)(makeProduceRequest(_, serializer, now, shardMap))
      _        <- queue.offerAll(requests.map(_._2))
      results  <- ZIO.foreach(requests)(_._1)
      latencies = results.map(r => java.time.Duration.between(now, r.completed))
      _        <- currentMetrics.getAndUpdate(_.addSuccesses(results.map(_.attempts), latencies))
    } yield results)
      .provide(env)
}

private[client] object ProducerLive {
  type ShardId      = String
  type PartitionKey = String

  val maxChunkSize: Int             = 512             // Stream-internal max chunk size
  val maxRecordsPerRequest          = 500             // This is a Kinesis API limitation
  val maxPayloadSizePerRequest      = 5 * 1024 * 1024 // 5 MB
  val maxPayloadSizePerRecord       = 1 * 1024 * 1024 // 1 MB
  val maxIngestionPerShardPerSecond = 1 * 1024 * 1024 // 1 MB
  val maxRecordsPerShardPerSecond   = 1000

  val recoverableErrorCodes = Set("ProvisionedThroughputExceededException", "InternalFailure", "ServiceUnavailable")

  final case class ProduceRequest(
    r: PutRecordsRequestEntry,
    complete: ZIO[Any, Throwable, ProduceResponse] => UIO[Unit],
    timestamp: Instant,
    predictedShard: ShardId,
    attemptNumber: Int = 1,
    isAggregated: Boolean = false,
    aggregateCount: Int = 1
  ) {
    def newAttempt = copy(attemptNumber = attemptNumber + 1)

    def isRetry: Boolean = attemptNumber > 1

    def payloadSize: Int = r.data().asByteArrayUnsafe().length + r.partitionKey().length
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
      entry          = PutRecordsRequestEntry
                .builder()
                .partitionKey(r.partitionKey)
                .data(SdkBytes.fromByteBuffer(data))
                .build()
      predictedShard = shardMap.shardForPartitionKey(Option(entry.explicitHashKey()).getOrElse(entry.partitionKey()))
    } yield (done.await, ProduceRequest(entry, done.completeWith(_).unit, now, predictedShard))

  final def scheduleCatchRecoverable: Schedule[Any, Throwable, Throwable] =
    Schedule.recurWhile {
      case e: KinesisException if e.statusCode() / 100 != 4 => true
      case _: ReadTimeoutException                          => true
      case _: IOException                                   => true
      case _                                                => false
    }

  def payloadSizeForEntry(entry: PutRecordsRequestEntry): Int =
    entry.partitionKey().length + entry.data().asByteArray().length

  def payloadSizeForEntryAggregated(entry: PutRecordsRequestEntry): Int =
    payloadSizeForEntry(entry) +
      3 +                                                                // Data
      3 + 2 +                                                            // Partition key
      Option(entry.explicitHashKey()).map(_.length + 2).getOrElse(1) + 3 // Explicit hash key

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

  val batcher: ZTransducer[Any, Nothing, ProduceRequest, Seq[ProduceRequest]] =
    foldWhile(PutRecordsBatch.empty)(_.isWithinLimits) { (batch, record: ProduceRequest) =>
      batch.add(record)
    }.map(_.entriesInOrder)

  val aggregator: ZTransducer[Any, Nothing, ProduceRequest, ProduceRequest] =
    foldWhile(PutRecordsAggregatedBatchForShard.empty)(_.isWithinLimits) { (batch, record: ProduceRequest) =>
      batch.add(record)
    }.mapM(_.toProduceRequest)
}
