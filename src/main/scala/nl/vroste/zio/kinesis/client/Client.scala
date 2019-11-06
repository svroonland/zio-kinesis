package nl.vroste.zio.kinesis.client

import java.time.Instant
import java.util.concurrent.CompletableFuture

import nl.vroste.zio.kinesis.client.serde.Deserializer
import software.amazon.awssdk.core.async.SdkPublisher
import software.amazon.awssdk.services.kinesis.model._
import software.amazon.awssdk.services.kinesis.{ KinesisAsyncClient, KinesisAsyncClientBuilder }
import zio._
import zio.clock.Clock
import zio.interop.javaz._
import zio.interop.reactiveStreams._
import zio.stream.{ Stream, ZStream }

import scala.collection.JavaConverters._

class Client(val kinesisClient: KinesisAsyncClient) {
  import Client._
  import Util._

  def listStreams(request: ListStreamsRequest): Task[ListStreamsResponse] =
    asZIO(kinesisClient.listStreams(request))

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
  def listShards(request: ListShardsRequest, fetchSize: Int = 10000): Stream[Throwable, Shard] =
    paginatedRequest { token =>
      asZIO {
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
        kinesisClient.listShards(requestWithToken)
      }.map(response => (response.shards().asScala, Option(response.nextToken())))
    }.mapConcatChunk(Chunk.fromIterable)

  def getShardIterator(request: GetShardIteratorRequest): Task[GetShardIteratorResponse] =
    asZIO(kinesisClient.getShardIterator(request))

  def putRecord(request: PutRecordRequest): Task[PutRecordResponse] =
    asZIO(kinesisClient.putRecord(request))

  def putRecords(request: PutRecordsRequest): Task[PutRecordsResponse] =
    asZIO(kinesisClient.putRecords(request))

  /**
   * Registers a stream consumer for use during the lifetime of the managed resource
   *
   * @param consumerName Name of the consumer
   * @param streamARN    ARN of the stream to consume
   * @return Managed resource that unregisters the stream consumer after use
   */
  def createConsumer(consumerName: String, streamARN: String): ZManaged[Any, Throwable, Consumer] =
    registerStreamConsumer(_.consumerName(consumerName).streamARN(streamARN))
      .toManaged(consumer => deregisterStreamConsumer(consumer.consumerARN()).ignore)

  /**
   * Creates a [[ZStream]] of the records in the given shard
   *
   * Records are deserialized to values of type [[T]]
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
  ): ZStream[R with Clock, Throwable, ConsumerRecord[T]] =
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
      deserializer.deserialize(record.data().asByteArray()).map { data =>
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
    builder: RegisterStreamConsumerRequest.Builder => RegisterStreamConsumerRequest.Builder
  ): ZIO[Any, Throwable, Consumer] =
    asZIO {
      kinesisClient.registerStreamConsumer(builder(RegisterStreamConsumerRequest.builder()).build())
    }.map(_.consumer())

  /**
   * @see [[createConsumer]] for automatic deregistration of the consumer
   */
  def deregisterStreamConsumer(consumerARN: String): ZIO[Any, Throwable, DeregisterStreamConsumerResponse] = asZIO {
    kinesisClient.deregisterStreamConsumer(r => {
      r.consumerARN(consumerARN);
      ()
    })
  }
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

}

private object Util {
  def asZIO[T](f: => CompletableFuture[T]): Task[T] = ZIO.fromCompletionStage(ZIO(f).orDie)

  // To avoid 'Discarded non-Unit value' warnings
  def consumer[T](f: T => T): java.util.function.Consumer[T] = x => {
    f(x)
    ()
  }

  type Token = String
  def paginatedRequest[R, E, A](fetch: Option[Token] => ZIO[R, E, (A, Option[Token])]): ZStream[R, E, A] =
    ZStream.fromEffect(fetch(None)).flatMap {
      case (results, nextTokenOpt) =>
        ZStream.succeed(results) ++ (nextTokenOpt match {
          case None => ZStream.empty
          case Some(nextToken) =>
            ZStream.paginate[R, E, A, Token](nextToken)(token => fetch(Some(token)))
        })
    }
}
