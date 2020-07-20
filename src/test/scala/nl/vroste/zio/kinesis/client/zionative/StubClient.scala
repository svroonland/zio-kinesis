package nl.vroste.zio.kinesis.client.zionative
import java.time.Instant

import nl.vroste.zio.kinesis.client.Client
import nl.vroste.zio.kinesis.client.serde.Serializer
import zio.clock.Clock
import zio.stream.ZStream
import zio.{ Task, ZIO, ZManaged }

class StubClient extends Client.Service {
  import software.amazon.awssdk.services.kinesis.model.{
    Consumer,
    ConsumerDescription,
    GetRecordsResponse,
    PutRecordResponse,
    PutRecordsRequestEntry,
    PutRecordsResponse,
    Shard,
    SubscribeToShardEvent
  }

  override def createConsumer(streamARN: String, consumerName: String): ZManaged[Any, Throwable, Consumer] =
    ???

  override def describeStreamConsumer(
    streamARN: String,
    consumerName: String
  ): ZIO[Any, Throwable, ConsumerDescription] = ???

  override def listShards(
    streamName: String,
    streamCreationTimestamp: Option[Instant],
    chunkSize: Int
  ): ZStream[Clock, Throwable, Shard] = ???

  override def getShardIterator(
    streamName: String,
    shardId: String,
    iteratorType: Client.ShardIteratorType
  ): Task[String] = ???

  override def subscribeToShard(
    consumerARN: String,
    shardID: String,
    startingPosition: Client.ShardIteratorType
  ): ZStream[Any, Throwable, SubscribeToShardEvent] = ???

  override def registerStreamConsumer(streamARN: String, consumerName: String): ZIO[Any, Throwable, Consumer] =
    ???

  override def deregisterStreamConsumer(consumerARN: String): Task[Unit] = Task.unit

  override def getRecords(shardIterator: String, limit: Int): Task[GetRecordsResponse] = ???

  override def putRecord[R, T](
    streamName: String,
    serializer: Serializer[R, T],
    r: Client.ProducerRecord[T]
  ): ZIO[R, Throwable, PutRecordResponse]  = ???
  override def putRecords[R, T](
    streamName: String,
    serializer: Serializer[R, T],
    records: Iterable[Client.ProducerRecord[T]]
  ): ZIO[R, Throwable, PutRecordsResponse] = ???
  override def putRecords(
    streamName: String,
    entries: List[PutRecordsRequestEntry]
  ): Task[PutRecordsResponse]              = ???
}
