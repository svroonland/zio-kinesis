package nl.vroste.zio.kinesis.client

import java.time.Instant
import java.util.concurrent.CompletableFuture

import nl.vroste.zio.kinesis.client.serde.{ Deserializer, Serializer }
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model._
import zio.clock.Clock
import zio.stream.ZStream
import zio.{ Has, Schedule, Task, ZIO, ZLayer, ZManaged }

object Client {

  val live: ZLayer[Has[KinesisAsyncClient], Throwable, Client] =
    ZLayer.fromService[KinesisAsyncClient, Client.Service] {
      new ClientLive(_)
    }

  /**
   * Client for consumer and producer operations
   *
   * The interface is as close as possible to the natural ZIO variant of the KinesisAsyncClient interface,
   * with some noteable differences:
   * - Methods working with records (consuming or producing) make use of Serdes for (de)serialization
   * - Paginated APIs such as listShards are modeled as a Stream
   * - AWS SDK library method responses that only indicate success and do not contain any other
   *   data (besides SDK internals) are mapped to a ZIO of Unit
   * - AWS SDK library method responses that contain a single field are mapped to a ZIO of that field's type
   *
   */
  trait Service {

    /**
     * Registers a stream consumer for use during the lifetime of the managed resource
     *
     * @param streamARN    ARN of the stream to consume
     * @param consumerName Name of the consumer
     * @return Managed resource that unregisters the stream consumer after use
     */
    def createConsumer(streamARN: String, consumerName: String): ZManaged[Any, Throwable, Consumer]

    /**
     * List all shards in a stream
     *
     * Handles paginated responses from the AWS API in a streaming manner
     *
     * @param chunkSize Number of results to fetch in one request (maxResults parameter).
     *                  Overwrites the maxResults in the request
     *
     * @return ZStream of shards in a stream
     */
    def listShards(
      streamName: String,
      streamCreationTimestamp: Option[Instant] = None,
      chunkSize: Int = 10000
    ): ZStream[Clock, Throwable, Shard]

    def getShardIterator(
      streamName: String,
      shardId: String,
      iteratorType: ShardIteratorType
    ): Task[String]

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
      startingPosition: ShardIteratorType,
      deserializer: Deserializer[R, T]
    ): ZStream[R, Throwable, ConsumerRecord[T]]

    /**
     * @see [[createConsumer]] for automatic deregistration of the consumer
     */
    def registerStreamConsumer(
      streamARN: String,
      consumerName: String
    ): ZIO[Any, Throwable, Consumer]

    /**
     * @see [[createConsumer]] for automatic deregistration of the consumer
     */
    def deregisterStreamConsumer(consumerARN: String): Task[Unit]

    def putRecord[R, T](
      streamName: String,
      serializer: Serializer[R, T],
      r: ProducerRecord[T]
    ): ZIO[R, Throwable, PutRecordResponse]

    def putRecords[R, T](
      streamName: String,
      serializer: Serializer[R, T],
      records: Iterable[ProducerRecord[T]]
    ): ZIO[R, Throwable, PutRecordsResponse]

    def putRecords(streamName: String, entries: List[PutRecordsRequestEntry]): Task[PutRecordsResponse]
  }

  case class ConsumerRecord[T](
    sequenceNumber: String,
    approximateArrivalTimestamp: Instant,
    data: T,
    partitionKey: String,
    encryptionType: EncryptionType,
    shardID: String
  )

  case class ProducerRecord[T](partitionKey: String, data: T)

  sealed trait ShardIteratorType
  object ShardIteratorType {
    case object Latest                                     extends ShardIteratorType
    case object TrimHorizon                                extends ShardIteratorType
    case class AtSequenceNumber(sequenceNumber: String)    extends ShardIteratorType
    case class AfterSequenceNumber(sequenceNumber: String) extends ShardIteratorType
    case class AtTimestamp(timestamp: Instant)             extends ShardIteratorType
  }

}

private object Util {
  def asZIO[T](f: => CompletableFuture[T]): Task[T] = ZIO.fromCompletionStage(f)

  type Token = String

  def paginatedRequest[R, E, A](fetch: Option[Token] => ZIO[R, E, (A, Option[Token])])(
    throttling: Schedule[Clock, Any, Int] = Schedule.forever
  ): ZStream[Clock with R, E, A] =
    ZStream.fromEffect(fetch(None)).flatMap {
      case (results, nextTokenOpt) =>
        ZStream.succeed(results) ++ (nextTokenOpt match {
          case None            => ZStream.empty
          case Some(nextToken) =>
            ZStream.paginateM[R, E, A, Token](nextToken)(token => fetch(Some(token))).scheduleElements(throttling)
        })
    }
}
