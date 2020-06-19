package nl.vroste.zio.kinesis.client

import java.time.Instant

import nl.vroste.zio.kinesis.client.serde.{ Deserializer, Serializer }
import software.amazon.awssdk.services.kinesis.model.{ Consumer, PutRecordResponse, PutRecordsResponse, Shard }
import zio.clock.Clock
import zio.stream.ZStream
import zio.{ Has, Task, ZIO, ZManaged }

object Client2 {
  import Client._

  type Client2 = Has[Service]
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

  }
}
