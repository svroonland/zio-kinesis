package nl.vroste.zio.kinesis.client

import java.time.Instant
import java.util.concurrent.CompletableFuture

import nl.vroste.zio.kinesis.client.Consumer.ConsumerRecord
import nl.vroste.zio.kinesis.client.Util._
import nl.vroste.zio.kinesis.client.serde.Deserializer
import software.amazon.awssdk.core.async.SdkPublisher
import software.amazon.awssdk.services.kinesis.model.{
  DeregisterStreamConsumerResponse,
  EncryptionType,
  GetShardIteratorRequest,
  GetShardIteratorResponse,
  ListShardsRequest,
  ListStreamsRequest,
  ListStreamsResponse,
  PutRecordRequest,
  PutRecordResponse,
  PutRecordsRequest,
  PutRecordsResponse,
  Record,
  RegisterStreamConsumerRequest,
  ResourceInUseException,
  Shard,
  StartingPosition,
  SubscribeToShardEvent,
  SubscribeToShardEventStream,
  SubscribeToShardRequest,
  SubscribeToShardResponse,
  SubscribeToShardResponseHandler,
  Consumer => KConsumer
}
import software.amazon.awssdk.services.kinesis.{ KinesisAsyncClient, KinesisAsyncClientBuilder }
import zio.clock.Clock
import zio.duration._
import zio.interop.javaz._
import zio.interop.reactiveStreams._
import zio.stream.{ Stream, ZStream }
import zio.{ Chunk, Promise, Task, ZIO, ZManaged, ZSchedule }

import scala.collection.JavaConverters._

class Client(val kinesisClient: KinesisAsyncClient) {
  import Client._

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
  def createConsumer(consumerName: String, streamARN: String): ZManaged[Any, Throwable, KConsumer] =
    registerStreamConsumer(_.consumerName(consumerName).streamARN(streamARN))
      .toManaged(consumer => deregisterStreamConsumer(consumer.consumerARN()).ignore)

  /**
   * Creates a stream of streams for each shard in a stream
   *
   * The subscription for each shard is automatically renewed
   *
   * @param consumerARN
   * @param streamName
   * @param shardStartingPositions
   * @param serde
   * @tparam R
   * @tparam T
   * @return
   */
  def consumeStream[R, T](
    consumerARN: String,
    streamName: String,
    shardStartingPositions: String => Task[StartingPosition],
    serde: Deserializer[R, T]
  ): ZStream[Clock, Throwable, ZStream[R with Clock, Throwable, ConsumerRecord[T]]] =
    listShards(ListShardsRequest.builder().streamName(streamName).build())
      .mapM(shard => shardStartingPositions(shard.shardId()).map((shard, _)))
      .map {
        case (shard, startingPosition) =>
          consumeShard(
            consumerARN,
            shard.shardId(),
            startingPosition,
            serde
          )
      }

  /**
   * Creates a [[ZStream]] of the records in the given shard
   *
   * @param consumerARN
   * @param shardID
   * @param startingPosition
   * @param serde
   * @tparam R
   * @tparam T
   * @return
   */
  def consumeShard[R, T](
    consumerARN: String,
    shardID: String,
    startingPosition: StartingPosition,
    serde: Deserializer[R, T]
  ): ZStream[R with Clock, Throwable, ConsumerRecord[T]] =
    ZStream.fromEffect {
      for {
        streamP <- Promise.make[Throwable, ZStream[Any, Throwable, Record]]
        runtime <- ZIO.runtime[Any]

        subscribeResponse = asZIO {
          kinesisClient.subscribeToShard(
            builder => {
              builder.consumerARN(consumerARN).shardId(shardID).startingPosition(startingPosition);
              ()
            },
            subscribeToShardResponseHandler(runtime, streamP)
          )
        }.map { r =>
          println(s"Got subscribe to shard response: ${r}")
          r
        }.retry(ZSchedule.exponential(1.second) && ZSchedule.recurs(3) && ZSchedule.doWhile[Throwable] {
          case _: ResourceInUseException => true
          case _                         => false
        })
        // subscribeResponse only completes with failure, not with success. It does not contain information of value anyway
        _      <- subscribeResponse.unit race streamP.await
        stream <- streamP.await
      } yield stream
    }.flatMap(identity)
      .mapM { record =>
        serde.deserialize(record.data().asByteArray()).map { data =>
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
      .repeat(ZSchedule.forever) // Subscription stops after 5 minutes

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

  def registerStreamConsumer(
    builder: RegisterStreamConsumerRequest.Builder => RegisterStreamConsumerRequest.Builder
  ): ZIO[Any, Throwable, KConsumer] =
    asZIO {
      kinesisClient.registerStreamConsumer(builder(RegisterStreamConsumerRequest.builder()).build())
    }.map(_.consumer())

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

  // To avoid 'Discarded non-Unit value' warnings
  private def consumer[T](f: T => T): java.util.function.Consumer[T] = x => {
    f(x)
    ()
  }

  type Token = String
  private def paginatedRequest[R, E, A](fetch: Option[Token] => ZIO[R, E, (A, Option[Token])]): ZStream[R, E, A] =
    ZStream.fromEffect(fetch(None)).flatMap {
      case (results, nextTokenOpt) =>
        ZStream.succeed(results) ++ (nextTokenOpt match {
          case None => ZStream.empty
          case Some(nextToken) =>
            ZStream.paginate[R, E, A, Token](nextToken)(token => fetch(Some(token)))
        })
    }
}

object Consumer {

  case class ConsumerRecord[T](
    sequenceNumber: String,
    approximateArrivalTimestamp: Instant,
    data: T,
    partitionKey: String,
    encryptionType: EncryptionType,
    shardID: String
  )

}

object Util {
  def asZIO[T](f: => CompletableFuture[T]): Task[T] = ZIO.fromCompletionStage(ZIO(f).orDie)

  def withClientAsZIO[E, T](f: KinesisAsyncClient => CompletableFuture[T]): ZIO[KinesisAsyncClient, Throwable, T] =
    ZIO.environment[KinesisAsyncClient].flatMap(client => asZIO(f(client)))
}
