package nl.vroste.zio.kinesis.client.producer

import io.netty.handler.timeout.ReadTimeoutException
import nl.vroste.zio.kinesis.client.Producer.ProduceResponse
import nl.vroste.zio.kinesis.client._
import nl.vroste.zio.kinesis.client.producer.ProducerLive._
import nl.vroste.zio.kinesis.client.serde.Serializer
import software.amazon.awssdk.core.exception.SdkException
import software.amazon.awssdk.services.kinesis.model.{ KinesisException, ResourceInUseException }
import zio.Clock.instant
import zio._
import zio.aws.kinesis.Kinesis
import zio.aws.kinesis.model.primitives.PartitionKey
import zio.aws.kinesis.model.{ PutRecordsRequest, PutRecordsRequestEntry, PutRecordsResponse, PutRecordsResultEntry }
import zio.stream.{ ZChannel, ZSink, ZStream }

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant
import scala.util.control.NonFatal
import nl.vroste.zio.kinesis.client.Producer.{ Aggregation, RichShardPrediction, RichThrottling }

private[client] final class ProducerLive[R, R1, T](
  client: Kinesis,
  env: ZEnvironment[R],
  queue: Queue[ProduceRequest],
  failedQueue: Queue[ProduceRequest],
  serializer: Serializer[R, T],
  currentMetrics: Ref[CurrentMetrics],
  settings: ProducerSettings,
  streamIdentifier: StreamIdentifier,
  metricsCollector: ProducerMetrics => ZIO[R1, Nothing, Unit],
  inFlightCalls: Ref[Int],
  shardPrediction: RichShardPrediction,
  throttling: RichThrottling
) extends Producer[T] {
  import Util.ZStreamExtensions
  import ProducerLive._

  val runloop: ZIO[Any, Nothing, Unit] = {
    val retries         = ZStream.fromQueue(failedQueue, maxChunkSize = maxChunkSize)
    val chunkBufferSize = Math.ceil(settings.bufferSize * 1.0 / maxChunkSize).toInt

    val newRequests = ZStream
      .fromQueue(queue, maxChunkSize)
      // Potentially add shard prediction to requests. If disabled predictedShard will be null
      .viaMatch(shardPrediction) { case enabled: RichShardPrediction.Enabled =>
        _.mapChunks(Chunk.single)
          .mapZIOParUnordered(enabled.parallelism)(addPredictedShardToRequestsChunk(enabled.md5Pool, enabled.shards))
          .flattenChunks
      }
      // Potentially aggregate records into KPL style aggregated records
      .viaMatch((settings.aggregation, shardPrediction)) {
        case (Aggregation.ByPredictedShard(timeout), RichShardPrediction.Enabled(_, md5Pool, _, _)) =>
          _.groupByKey(_.predictedShard, chunkBufferSize)(
            { case (shardId @ _, requests) =>
              ZStream.scoped(md5Pool.get).flatMap { digest =>
                requests
                  .aggregateAsyncWithinDuration(aggregator, timeout)
                  .mapConcatZIO(_.toProduceRequest(digest).map(_.toList))
              }
            },
            chunkBufferSize
          )
      }

    retries
      .merge(newRequests)
      // Potentially throttle requests per shard
      .viaMatch(throttling) { case RichThrottling.Enabled(throttler) =>
        _.groupByKey(_.predictedShard, chunkBufferSize)(throttleShardRequests(throttler), chunkBufferSize)
      }
      // If timeout has been provided batch records up to the timeout / Kinesis PutRecords request limits.
      // Otherwise batch records up to the Kinesis PutRecords request limits as long as downstream is busy.
      .aggregateAsyncWithinDuration(batcher, settings.batchDuration)
      .filter(_.nonEmpty) // TODO why would this be necessary?
      // Several putRecords requests in parallel
      .flatMapPar(settings.maxParallelRequests, chunkBufferSize)(b => ZStream.fromZIO(countInFlight(processBatch(b))))
      .collect { case (Some(response), requests) => (response, requests) }
      .mapZIO((processBatchResponse _).tupled)
      .tap(metrics => currentMetrics.update(_ append metrics))
      .runDrain
      .orDie
  }

  private def addPredictedShardToRequestsChunk(md5Pool: ZPool[Nothing, MessageDigest], shards: Ref[ShardMap])(
    chunk: Chunk[ProduceRequest]
  ) =
    ZIO.scoped {
      (md5Pool.get zip shards.get).flatMap { case (md5, shardMap) =>
        chunk.mapZIO { r =>
          ZIO
            .attempt(shardMap.shardForPartitionKey(md5, r.partitionKey))
            .map(shard => r.copy(predictedShard = shard))
        }
      }
    }

  private def throttleShardRequests(
    throttler: ShardThrottler
  )(shardId: ShardId, requests: ZStream[Any, Throwable, ProduceRequest]) =
    ZStream.fromZIO(throttler.getForShard(shardId)).flatMap { throttlerForShard =>
      requests
        .mapChunks(
          _.map { request =>
            request
              .copy(complete =
                result => request.complete(result) *> result.zipLeft(throttlerForShard.addSuccess).ignore
              )
          }
        )
        .throttleShapeZIO(maxRecordsPerShardPerSecond.toLong, 1.second)(chunk =>
          throttlerForShard.throughputFactor.map(c => (chunk.size * 1.0 / c).toLong)
        )
        .throttleShapeZIO(maxIngestionPerShardPerSecond.toLong, 1.second)(chunk =>
          throttlerForShard.throughputFactor.map(c => (chunk.map(_.payloadSize).sum * 1.0 / c).toLong)
        )
    }

  private def processBatch(
    batch: Chunk[ProduceRequest]
  ): ZIO[Any, Nothing, (Option[PutRecordsResponse.ReadOnly], Chunk[ProduceRequest])] = {
    val totalPayload = batch.map(_.data.length).sum
    (for {
      _ <- ZIO.logInfo(
             s"PutRecords for batch of size ${batch.map(_.aggregateCount).sum} (${batch.size} aggregated). " +
//               s"Payload sizes: ${batch.map(_.data.asByteArrayUnsafe().length).mkString(",")} " +
               s"(total = ${totalPayload} = ${totalPayload * 100.0 / maxPayloadSizePerRequest}%)."
           )

      // Avoid an allocation
      response <-
        client
          .putRecords(
            new PutRecordsRequest(batch.map(_.asPutRecordsRequestEntry), streamIdentifier.name, streamIdentifier.arn)
          )
          .mapError(_.toThrowable)
          .tapError(e => ZIO.logWarning(s"Error producing records, will retry if recoverable: $e"))
          .retry(scheduleCatchRecoverable && settings.backoffRequests)
    } yield (Some(response), batch)).catchSome { case NonFatal(e) =>
      ZIO.logWarning("Failed to process batch") *>
        ZIO.foreachDiscard(batch)(_.complete(ZIO.fail(e))).as((None, batch))
    }.orDie
  }

  private def processBatchResponse(
    response: PutRecordsResponse.ReadOnly,
    batch: Chunk[ProduceRequest]
  ): ZIO[Any, Nothing, CurrentMetrics] = {
    val totalPayload = batch.map(_.data.length).sum

    val responseAndRequests    = Chunk.fromIterable(response.records).zip(batch)
    val (newFailed, succeeded) =
      if (response.failedRecordCount.getOrElse(0) > 0)
        responseAndRequests.partition { case (result, _) =>
          result.errorCode.exists(recoverableErrorCodes.contains)
        }
      else
        (Chunk.empty, responseAndRequests)
    for {

      now                           <- instant
      m1                             = CurrentMetrics.empty(now).addPayloadSize(totalPayload).addRecordSizes(batch.map(_.payloadSize))
      r                             <- checkShardPredictionErrors(responseAndRequests, m1)
      (hasShardPredictionErrors, m2) = r
      m3                            <- handleFailures(newFailed, repredict = hasShardPredictionErrors, m2)
      _                             <- currentMetrics.getAndUpdate(succeeded.foldLeft(_) { case (metrics, (_, request)) =>
                                         if (request.isAggregated) {
                                           metrics.addSuccesses(
                                             Chunk.fill(request.aggregateCount)(request.attemptNumber),
                                             Chunk.fill(request.aggregateCount)(java.time.Duration.between(request.timestamp, now))
                                           )
                                         } else {
                                           metrics.addSuccess(request.attemptNumber, java.time.Duration.between(request.timestamp, now))
                                         }
                                       })
      _                             <- ZIO.foreachDiscard(succeeded) { case (response, request) =>
                                         request.complete(
                                           ZIO.succeed(
                                             ProduceResponse(
                                               response.shardId.toOption.get,
                                               response.sequenceNumber.toOption.get,
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
      _ <- ZIO.whenCase(throttling) { case RichThrottling.Enabled(throttler) =>
             ZIO.foreachDiscard(requests)(r => throttler.getForShard(r.predictedShard).flatMap(_.addFailure))
           }
      _ <- ZIO.logWarning(s"Failed to produce ${failedCount} records").when(newFailed.nonEmpty)
      _ <- ZIO.logWarning(responses.take(10).flatMap(_.errorCode.toOption).mkString(", ")).when(newFailed.nonEmpty)

      // The shard map may not yet be updated unless we're experiencing high latency
      updatedFailed <- if (repredict)
                         shardPrediction match {
                           case enabled: RichShardPrediction.Enabled =>
                             ZIO.scoped {
                               enabled.md5Pool.get.flatMap { digest =>
                                 enabled.shards.get.map(shardMap =>
                                   requests.map(r =>
                                     r.newAttempt
                                       .copy(predictedShard = shardMap.shardForPartitionKey(digest, r.partitionKey))
                                   )
                                 )
                               }
                             }
                           case _                                    =>
                             ZIO.dieMessage("Shard prediction is disabled")
                         }
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
  ): ZIO[Any, Nothing, (Boolean, CurrentMetrics)] = {
    val shardPredictionErrors = responseAndRequests.filter { case (result, request) =>
      (request.predictedShard != null) && result.shardId.exists(_ != request.predictedShard)
    }

    val (succeeded, failed) = shardPredictionErrors.partition(_._1.errorCode.isEmpty)

    val updatedMetrics = metrics.addShardPredictionErrors(shardPredictionErrors.map(_._2.aggregateCount).sum.toLong)

    ZIO
      .when(shardPredictionErrors.nonEmpty) {
        shardPrediction match {
          case enabled: RichShardPrediction.Enabled =>
            val maxProduceRequestTimestamp = shardPredictionErrors
              .map(_._2.timestamp.toEpochMilli)
              .max

            ZIO.logWarning(
              s"${succeeded.map(_._2.aggregateCount).sum} records (aggregated as ${succeeded.size}) ended up " +
                s"on a different shard than expected and/or " +
                s"${failed.map(_._2.aggregateCount).sum} records (aggregated as ${failed.size}) would end up " +
                s"on a different shard than expected if they had succeeded. This may happen after a reshard."
            ) *> enabled.triggerUpdateShards.fork.whenZIO { // Fiber cannot fail
              enabled.shards
                .getAndUpdate(_.invalidate)
                .map(shardMap => !shardMap.invalid && shardMap.lastUpdated.toEpochMilli < maxProduceRequestTimestamp)
            }
          case _                                    =>
            ZIO.dieMessage("Shard prediction is disabled")
        }
      }
      .as((shardPredictionErrors.nonEmpty, updatedMetrics))
  }

  private def countInFlight[R0, E, A](e: ZIO[R0, E, A]): ZIO[R0, E, A] =
    ZIO.acquireReleaseWith(
      inFlightCalls
        .updateAndGet(_ + 1)
        .tap(inFlightCalls => ZIO.logDebug(s"${inFlightCalls} PutRecords calls in flight"))
    )(_ =>
      inFlightCalls
        .updateAndGet(_ - 1)
        .tap(inFlightCalls => ZIO.logDebug(s"${inFlightCalls} PutRecords calls in flight"))
    )(_ => e)

  val collectMetrics: ZIO[R1, Nothing, Unit] = for {
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
  val metricsCollection: ZIO[R1, Nothing, Long] = collectMetrics
    .delay(settings.metricsInterval)
    .repeat(Schedule.fixed(settings.metricsInterval))

  override def produceAsync(r: ProducerRecord[T]): Task[Task[ProduceResponse]] =
    (for {
      now             <- instant
      ar              <- makeProduceRequest(r, serializer, now)
      (await, request) = ar
      _               <- queue.offer(request)
    } yield await).provideEnvironment(env)

  override def produceChunkAsync(chunk: Chunk[ProducerRecord[T]]): Task[Task[Chunk[ProduceResponse]]] =
    (for {
      now <- instant

      done              <- Promise.make[Throwable, Chunk[ProduceResponse]]
      resultsCollection <- Ref.make[Chunk[ProduceResponse]](Chunk.empty)
      nrRequests         = chunk.size
      onDone             = (response: Task[ProduceResponse]) =>
                             response
                               .foldZIO(
                                 done.fail,
                                 response =>
                                   for {
                                     responses <- resultsCollection.updateAndGet(_ :+ response)
                                     _         <- ZIO.when(responses.size == nrRequests)(done.succeed(responses))
                                   } yield ()
                               )
                               .unit
      requests          <- ZIO.foreach(chunk) { r =>
                             for {
                               data <- serializer.serialize(r.data)
                             } yield (done.await, ProduceRequest(data, PartitionKey(r.partitionKey), onDone, now, null))
                           }
      _                 <- queue.offerAll(requests.map(_._2))
      await              = if (chunk.nonEmpty) done.await else ZIO.succeed(Chunk.empty)
    } yield await).provideEnvironment(env)
}

private[client] object ProducerLive {
  type ShardId = String

  val maxChunkSize: Int             = 1024            // Stream-internal max chunk size
  val maxRecordsPerRequest          = 500             // This is a Kinesis API limitation
  val maxPayloadSizePerRequest      = 5 * 1024 * 1024 // 5 MB
  val maxPayloadSizePerRecord       =
    1 * 1024 * 921 // 1 MB TODO actually 90%, to avoid underestimating and getting Kinesis errors
  val maxIngestionPerShardPerSecond = 1 * 1024 * 1024 // 1 MB
  val maxRecordsPerShardPerSecond   = 1000

  val recoverableErrorCodes = Set("ProvisionedThroughputExceededException", "InternalFailure", "ServiceUnavailable")

  final case class ProduceRequest(
    data: Chunk[Byte],
    partitionKey: PartitionKey,
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

    def asPutRecordsRequestEntry: PutRecordsRequestEntry =
      PutRecordsRequestEntry(zio.aws.kinesis.model.primitives.Data.apply(data), partitionKey = partitionKey)
  }

  def makeProduceRequest[R, T](
    r: ProducerRecord[T],
    serializer: Serializer[R, T],
    now: Instant
  ): ZIO[R, Throwable, (ZIO[Any, Throwable, ProduceResponse], ProduceRequest)] =
    for {
      done <- Promise.make[Throwable, ProduceResponse]
      data <- serializer.serialize(r.data)
    } yield (
      done.await,
      ProduceRequest(data, PartitionKey(r.partitionKey), done.completeWith(_).unit, now, predictedShard = null)
    )

  final def scheduleCatchRecoverable: Schedule[Any, Throwable, Throwable] =
    Schedule.recurWhile(isRecoverableException)

  private def isRecoverableException(e: Throwable): Boolean =
    e match {
      case e: KinesisException if e.statusCode() / 100 != 4 => true
      case _: ReadTimeoutException                          => true
      case _: IOException                                   => true
      case _: ResourceInUseException                        =>
        true // Also covers DELETING, but will result in ResourceNotFoundException on a subsequent attempt
      case e: SdkException if Option(e.getCause).isDefined => isRecoverableException(e.getCause)
      case _                                               => false
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
  def foldWhile[Env, Err, In, S](z: => S)(contFn: S => Boolean)(
    f: (S, In) => ZIO[Env, Err, S]
  )(implicit trace: Trace): ZSink[Env, Err, In, In, S] =
    ZSink.suspend {
      def foldChunkSplitM(z: S, chunk: Chunk[In])(
        contFn: S => Boolean
      )(f: (S, In) => ZIO[Env, Err, S]): ZIO[Env, Err, (S, Option[Chunk[In]])] = {

        def fold(s: S, chunk: Chunk[In], idx: Int, len: Int): ZIO[Env, Err, (S, Option[Chunk[In]])] =
          if (idx == len) ZIO.succeed((s, None))
          else
            f(s, chunk(idx)).flatMap { s1 =>
              if (contFn(s1))
                fold(s1, chunk, idx + 1, len)
              else
                ZIO.succeed((s, Some(chunk.drop(idx))))
            }

        fold(z, chunk, 0, chunk.length)
      }

      def reader(s: S): ZChannel[Env, Err, Chunk[In], Any, Err, Chunk[In], S] =
        if (!contFn(s)) ZChannel.succeedNow(s)
        else
          ZChannel.readWith(
            (in: Chunk[In]) =>
              ZChannel.fromZIO(foldChunkSplitM(s, in)(contFn)(f)).flatMap { case (nextS, leftovers) =>
                leftovers match {
                  case Some(l) => ZChannel.write(l).as(nextS)
                  case None    => reader(nextS)
                }
              },
            (err: Err) => ZChannel.fail(err),
            (_: Any) => ZChannel.succeedNow(s)
          )

      ZSink.fromChannel(reader(z))
    }

  val batcher: ZSink[Any, Nothing, ProduceRequest, ProduceRequest, Chunk[ProduceRequest]] =
    foldWhile(PutRecordsBatch.empty)(_.isWithinLimits) { (batch, record: ProduceRequest) =>
      ZIO.succeed(batch.add(record))
    }.map(_.entries)

  val aggregator: ZSink[Any, Nothing, ProduceRequest, ProduceRequest, PutRecordsAggregatedBatchForShard] =
    foldWhile(PutRecordsAggregatedBatchForShard.empty)(_.isWithinLimits) { (batch, record: ProduceRequest) =>
      ZIO.succeed(batch.add(record))
    }
}
