package nl.vroste.zio.kinesis.client

import java.time.Instant
import java.util.concurrent.CompletableFuture

import nl.vroste.zio.kinesis.client.serde.{ Deserializer, Serializer }
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.core.async.SdkPublisher
import software.amazon.awssdk.services.kinesis.model._
import software.amazon.awssdk.services.kinesis.{ KinesisAsyncClient, KinesisAsyncClientBuilder }
import zio._
import zio.clock.Clock
import zio.duration._
import zio.interop.javaz._
import zio.interop.reactiveStreams._
import zio.stream.ZStream

import scala.collection.JavaConverters._

/**
 * ZIO wrapper for around the KinesisAsyncClient
 *
 * The interface is as close as possible to the natural ZIO variant of the KinesisAsyncClient interface,
 * with some noteable differences:
 * - Methods working with records (consuming or producing) make use of Serdes for (de)serialization
 * - Paginated APIs such as listShards are modeled as a Stream
 * - AWS SDK library method responses that only indicate success and do not contain any other
 *   data (besides SDK internals) are mapped to a ZIO of Unit
 * - AWS SDK library method responses that contain a single field are mapped to a ZIO of that field's type
 *
 * @param kinesisClient
 */
class Client(val kinesisClient: KinesisAsyncClient) {
  import Client._
  import Util._

  /**
   * Registers a stream consumer for use during the lifetime of the managed resource
   *
   * @param streamARN    ARN of the stream to consume
   * @param consumerName Name of the consumer
   * @return Managed resource that unregisters the stream consumer after use
   */
  def createConsumer(streamARN: String, consumerName: String): ZManaged[Any, Throwable, Consumer] =
    registerStreamConsumer(streamARN, consumerName)
      .toManaged(consumer => deregisterStreamConsumer(consumer.consumerARN()).ignore)

  /**
   * List all shards in a stream
   *
   * Handles paginated responses from the AWS API in a streaming manner
   *
   * @param request Request parameters
   * @param fetchSize Number of results to fetch in one request (maxResults parameter).
   *                  Overwrites the maxResults in the request
   *
   * @return ZStream of shards in a stream
   */
  def listShards(request: ListShardsRequest, fetchSize: Int = 10000): ZStream[Clock, Throwable, Shard] =
    paginatedRequest { token =>
      val requestWithToken =
        request.copy(
          consumer[ListShardsRequest.Builder](
            builder =>
              token
                .map(builder.nextToken(_).streamName(null))
                .getOrElse(builder)
                .maxResults(fetchSize)
          )
        )
      asZIO(kinesisClient.listShards(requestWithToken))
        .map(response => (response.shards().asScala, Option(response.nextToken())))
    }(Schedule.fixed(10.millis)).mapConcatChunk(Chunk.fromIterable)

  /**
   * Get a shard iterator at or after a sequence number
   *
   * @param streamName
   * @param shardId
   * @param iteratorType Must be AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER
   * @param startingSequenceNumber
   * @return
   */
  def getShardIterator(
    streamName: String,
    shardId: String,
    iteratorType: ShardIteratorType,
    startingSequenceNumber: String
  ): Task[String] = {
    val request = GetShardIteratorRequest
      .builder()
      .streamName(streamName)
      .shardId(shardId)
      .shardIteratorType(iteratorType)
      .startingSequenceNumber(startingSequenceNumber)
      .build()

    asZIO(kinesisClient.getShardIterator(request)).map(_.shardIterator())
  }

  /**
   * Get a shard iterator of type LATEST or TRIM_HORIZON
   *
   * @param streamName
   * @param shardId
   * @param iteratorType Must be LATEST or TRIM_HORIZON
   * @return
   */
  def getShardIterator(
    streamName: String,
    shardId: String,
    iteratorType: ShardIteratorType
  ): Task[String] = {
    val request = GetShardIteratorRequest
      .builder()
      .streamName(streamName)
      .shardId(shardId)
      .shardIteratorType(iteratorType)
      .build()

    asZIO(kinesisClient.getShardIterator(request)).map(_.shardIterator())
  }

  /**
   * Get a shard iterator of type AT_TIMESTAMP
   *
   * @param streamName
   * @param shardId
   * @param timestamp
   * @return
   */
  def getShardIterator(
    streamName: String,
    shardId: String,
    timestamp: Instant
  ): Task[String] = {
    val request = GetShardIteratorRequest
      .builder()
      .streamName(streamName)
      .shardId(shardId)
      .shardIteratorType(ShardIteratorType.AT_TIMESTAMP)
      .timestamp(timestamp)
      .build()

    asZIO(kinesisClient.getShardIterator(request)).map(_.shardIterator())
  }

  /**
   * Creates a `ZStream` of the records in the given shard
   *
   * Records are deserialized to values of type `T`
   *
   * Subscriptions are valid for only 5 minutes and should be renewed by the caller with
   * an up to date starting position.
   *
   * @param consumerARN
   * @param shardID
   * @param startingPosition
   * @param deserializer Converter of record's data bytes to a value of type T
   * @tparam R Environment required by the deserializer
   * @tparam T Type of values
   * @return Stream of records. When exceptions occur in the subscription or the streaming, the
   *         stream will fail.
   */
  def subscribeToShard[R, T](
    consumerARN: String,
    shardID: String,
    startingPosition: StartingPosition,
    deserializer: Deserializer[R, T]
  ): ZStream[R, Throwable, ConsumerRecord[T]] =
    ZStream.fromEffect {
      for {
        streamP <- Promise.make[Throwable, ZStream[Any, Throwable, Record]]
        runtime <- ZIO.runtime[Any]

        subscribeResponse = asZIO {
          kinesisClient.subscribeToShard(
            consumer[SubscribeToShardRequest.Builder](
              builder =>
                builder
                  .consumerARN(consumerARN)
                  .shardId(shardID)
                  .startingPosition(startingPosition)
            ),
            subscribeToShardResponseHandler(runtime, streamP)
          )
        }
        // subscribeResponse only completes with failure, not with success. It does not contain information of value anyway
        _      <- subscribeResponse.unit race streamP.await
        stream <- streamP.await
      } yield stream
    }.flatMap(identity).mapM { record =>
      deserializer.deserialize(record.data().asByteBuffer()).map { data =>
        ConsumerRecord(
          record.sequenceNumber(),
          record.approximateArrivalTimestamp(),
          data,
          record.partitionKey(),
          record.encryptionType(),
          shardID
        )
      }
    }

  private def subscribeToShardResponseHandler(
    runtime: zio.Runtime[Any],
    streamP: Promise[Throwable, ZStream[Any, Throwable, Record]]
  ) =
    new SubscribeToShardResponseHandler {
      override def responseReceived(response: SubscribeToShardResponse): Unit =
        ()

      override def onEventStream(publisher: SdkPublisher[SubscribeToShardEventStream]): Unit = {
        val streamOfRecords: ZStream[Any, Throwable, SubscribeToShardEvent] =
          publisher.filter(classOf[SubscribeToShardEvent]).toStream()
        runtime.unsafeRun(streamP.succeed(streamOfRecords.mapConcat(_.records().asScala)).unit)
      }

      override def exceptionOccurred(throwable: Throwable): Unit = ()

      override def complete(): Unit = () // We only observe the subscriber's onComplete
    }

  /**
   * @see [[createConsumer]] for automatic deregistration of the consumer
   */
  def registerStreamConsumer(
    streamARN: String,
    consumerName: String
  ): ZIO[Any, Throwable, Consumer] = {
    val request = RegisterStreamConsumerRequest.builder().streamARN(streamARN).consumerName(consumerName).build()
    asZIO(kinesisClient.registerStreamConsumer(request)).map(_.consumer())
  }

  /**
   * @see [[createConsumer]] for automatic deregistration of the consumer
   */
  def deregisterStreamConsumer(consumerARN: String): Task[Unit] = {
    val request = DeregisterStreamConsumerRequest.builder().consumerARN(consumerARN).build()
    asZIO(kinesisClient.deregisterStreamConsumer(request)).unit
  }

  private def putRecord(request: PutRecordRequest): Task[PutRecordResponse] =
    asZIO(kinesisClient.putRecord(request))

  private def putRecords(request: PutRecordsRequest): Task[PutRecordsResponse] =
    asZIO(kinesisClient.putRecords(request))

  def putRecord[R, T](
    streamName: String,
    serializer: Serializer[R, T],
    r: ProducerRecord[T]
  ): ZIO[R, Throwable, PutRecordResponse] =
    for {
      dataBytes <- serializer.serialize(r.data)
      request = PutRecordRequest
        .builder()
        .streamName(streamName)
        .partitionKey(r.partitionKey)
        .data(SdkBytes.fromByteBuffer(dataBytes))
        .build()
      response <- putRecord(request)
    } yield response

  def putRecords[R, T](
    streamName: String,
    serializer: Serializer[R, T],
    records: Iterable[ProducerRecord[T]]
  ): ZIO[R, Throwable, PutRecordsResponse] =
    for {
      recordsAndBytes <- ZIO.traverse(records)(r => serializer.serialize(r.data).map((_, r.partitionKey)))
      entries = recordsAndBytes.map {
        case (data, partitionKey) =>
          PutRecordsRequestEntry.builder().data(SdkBytes.fromByteBuffer(data)).partitionKey(partitionKey).build()
      }
      response <- putRecords(streamName, entries)
    } yield response

  def putRecords(streamName: String, entries: List[PutRecordsRequestEntry]): Task[PutRecordsResponse] =
    putRecords(PutRecordsRequest.builder().streamName(streamName).records(entries: _*).build())

}

object Client {

  /**
   * Create a client with the region and credentials from the default providers
   *
   * @return Managed resource that is closed after use
   */
  def create: ZManaged[Any, Throwable, Client] =
    ZManaged.fromAutoCloseable {
      ZIO.effect(KinesisAsyncClient.create())
    }.map(new Client(_))

  /**
   * Create a custom client
   *
   * @return Managed resource that is closed after use
   */
  def build(builder: KinesisAsyncClientBuilder): ZManaged[Any, Throwable, Client] =
    ZManaged.fromAutoCloseable {
      ZIO.effect(builder.build())
    }.map(new Client(_))

  case class ConsumerRecord[T](
    sequenceNumber: String,
    approximateArrivalTimestamp: Instant,
    data: T,
    partitionKey: String,
    encryptionType: EncryptionType,
    shardID: String
  )

  case class ProducerRecord[T](partitionKey: String, data: T)
}

private object Util {
  def asZIO[T](f: => CompletableFuture[T]): Task[T] = ZIO.fromCompletionStage(ZIO(f).orDie)

  // To avoid 'Discarded non-Unit value' warnings
  def consumer[T](f: T => T): java.util.function.Consumer[T] = x => {
    f(x)
    ()
  }

  type Token = String
  def paginatedRequest[R, E, A](fetch: Option[Token] => ZIO[R, E, (A, Option[Token])])(
    throttling: Schedule[Clock, Any, Int] = Schedule.forever
  ): ZStream[Clock with R, E, A] =
    ZStream.fromEffect(fetch(None)).flatMap {
      case (results, nextTokenOpt) =>
        ZStream.succeed(results) ++ (nextTokenOpt match {
          case None => ZStream.empty
          case Some(nextToken) =>
            ZStream.paginate[R, E, A, Token](nextToken)(token => fetch(Some(token))).scheduleElements(throttling)
        })
    }
}
