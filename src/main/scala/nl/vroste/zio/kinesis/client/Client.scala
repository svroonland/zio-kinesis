package nl.vroste.zio.kinesis.client

import java.time.Instant

import nl.vroste.zio.kinesis.client.serde.Serializer
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model._
import zio._
import zio.clock.Clock
import zio.stream.ZStream

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
   * - Paginated APIs such as listShards are modeled as a ZStream
   * - AWS SDK library method responses that only indicate success and do not contain any other
   *   data (besides SDK internals) are mapped to a ZIO of Unit
   * - AWS SDK library method responses that contain a single field are mapped to a ZIO of that field's type
   *
   */
  trait Service {

    /**
     * Registers a stream consumer for use during the lifetime of the managed resource
     *
     * If the consumer already exists, it will be reused
     *
     * @param streamARN    ARN of the stream to consume
     * @param consumerName Name of the consumer
     * @return Managed resource that unregisters the stream consumer after use
     */
    def createConsumer(streamARN: String, consumerName: String): ZManaged[Any, Throwable, Consumer]

    def describeStreamConsumer(streamARN: String, consumerName: String): ZIO[Any, Throwable, ConsumerDescription]

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
     * @return Stream of SubscribeToShardEvents, each of which contain records.
     *         When exceptions occur in the subscription or the streaming, the stream will fail.
     */
    def subscribeToShard(
      consumerARN: String,
      shardID: String,
      startingPosition: ShardIteratorType
    ): ZStream[Any, Throwable, SubscribeToShardEvent]

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

    def getRecords(shardIterator: String, limit: Int): Task[GetRecordsResponse]

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

  // Accessor methods
  def listShards(
    streamName: String,
    streamCreationTimestamp: Option[Instant] = None,
    chunkSize: Int = 10000
  ): ZStream[Clock with Client, Throwable, Shard] =
    ZStream.unwrap {
      ZIO.service[Service].map(_.listShards(streamName, streamCreationTimestamp, chunkSize))
    }

  def getShardIterator(streamName: String, shardId: String, iteratorType: ShardIteratorType): ClientTask[String] =
    ZIO.service[Client.Service].flatMap(_.getShardIterator(streamName, shardId, iteratorType))

  def getRecords(shardIterator: String, limit: Int): ClientTask[GetRecordsResponse] =
    ZIO.service[Client.Service].flatMap(_.getRecords(shardIterator, limit))

  type ClientTask[+A] = ZIO[Client, Throwable, A]

  final case class ProducerRecord[T](partitionKey: String, data: T)

  sealed trait ShardIteratorType
  object ShardIteratorType {
    case object Latest                                           extends ShardIteratorType
    case object TrimHorizon                                      extends ShardIteratorType
    final case class AtSequenceNumber(sequenceNumber: String)    extends ShardIteratorType
    final case class AfterSequenceNumber(sequenceNumber: String) extends ShardIteratorType
    final case class AtTimestamp(timestamp: Instant)             extends ShardIteratorType
  }
}
