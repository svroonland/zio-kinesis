package nl.vroste.zio.kinesis.client.producer

import io.netty.handler.timeout.ReadTimeoutException
import nl.vroste.zio.kinesis.client.Producer.ProduceResponse
import nl.vroste.zio.kinesis.client._
import nl.vroste.zio.kinesis.client.producer.ProducerLive.ProduceRequest
import nl.vroste.zio.kinesis.client.serde.Serializer
import software.amazon.awssdk.core.exception.SdkException
import software.amazon.awssdk.services.kinesis.model.{ KinesisException, ResourceInUseException }
import zio.Clock.instant
import zio._
import zio.aws.kinesis.Kinesis
import zio.aws.kinesis.model.primitives.{ PartitionKey, StreamName }
import zio.aws.kinesis.model.{ PutRecordsRequest, PutRecordsRequestEntry, PutRecordsResponse, PutRecordsResultEntry }
import zio.stream.{ ZChannel, ZSink, ZStream }

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant
import scala.annotation.unused
import scala.util.control.NonFatal

private[client] final class ProducerLive[R, R1, T](
  @unused client: Kinesis,
  env: ZEnvironment[R],
  queue: Queue[ProduceRequest],
  @unused failedQueue: Queue[ProduceRequest],
  serializer: Serializer[R, T],
  currentMetrics: Ref[CurrentMetrics],
  shards: Ref[ShardMap],
  @unused settings: ProducerSettings,
  @unused streamName: StreamName,
  @unused metricsCollector: ProducerMetrics => ZIO[R1, Nothing, Unit],
  @unused aggregate: Boolean = false,
  @unused inFlightCalls: Ref[Int],
  @unused triggerUpdateShards: UIO[Unit],
  @unused throttler: ShardThrottler,
  md5Pool: ZPool[Throwable, MessageDigest]
) extends Producer[T] {
  import ProducerLive._
  import Util.ZStreamExtensions

  val runloop: ZIO[Any, Nothing, Unit] =
    ZStream
      .fromQueue(queue, maxChunkSize)
      .mapChunksZIO(chunk => ZIO.logDebug(s"Dequeued chunk of size ${chunk.size}").as(Chunk.single(chunk)))
      .mapZIO(addPredictedShardToRequestsChunk)
      .flattenChunks
      .aggregateAsync(batcher)
      .mapZIO(processBatch)
      .runDrain
      .orDie

  private def addPredictedShardToRequestsChunk(chunk: Chunk[ProduceRequest]) =
    ZIO.scoped {
      (md5Pool.get zip shards.get).flatMap { case (md5, shardMap) =>
        chunk.mapZIO { r =>
          ZIO
            .attempt(shardMap.shardForPartitionKey(md5, r.partitionKey))
            .map(shard => r.copy(predictedShard = shard))
        }
      }
    }

  private def processBatch(
    @unused batch: Chunk[ProduceRequest]
  ): ZIO[Any, Nothing, Unit] =
    ZIO.logInfo("Downstream call")

  override def produce(r: ProducerRecord[T]): Task[ProduceResponse] =
    (for {
      now             <- instant
      ar              <- makeProduceRequest(r, serializer, now)
      (await, request) = ar
      _               <- queue.offer(request)
      response        <- await
      latency          = java.time.Duration.between(now, response.completed)
      _               <- currentMetrics.getAndUpdate(_.addSuccess(response.attempts, latency))
    } yield response).provideEnvironment(env)

  override def produceChunk(chunk: Chunk[ProducerRecord[T]]): Task[Chunk[ProduceResponse]] =
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
      results           <- if (chunk.nonEmpty) done.await else ZIO.succeed(Chunk.empty)
      latencies          = results.map(r => java.time.Duration.between(now, r.completed))
      _                 <- currentMetrics.getAndUpdate(_.addSuccesses(results.map(_.attempts), latencies))
    } yield results)
      .provideEnvironment(env)
}

private[client] object ProducerLive {
  type ShardId = String

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

      new ZSink(reader(z))
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
