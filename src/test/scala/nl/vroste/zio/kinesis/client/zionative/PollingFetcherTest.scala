package nl.vroste.zio.kinesis.client.zionative
import java.nio.charset.Charset
import java.time.Instant

import nl.vroste.zio.kinesis.client.Client
import nl.vroste.zio.kinesis.client.Client.ShardIteratorType
import nl.vroste.zio.kinesis.client.serde.Serializer
import nl.vroste.zio.kinesis.client.zionative.fetcher.PollingFetcher
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.model.{ GetRecordsResponse, Record }
import zio.clock.Clock
import zio.logging.Logging
import zio.stream.ZStream
import zio.test._
import zio.test.Assertion._
import zio._
import zio.duration.durationInt
import zio.logging.slf4j.Slf4jLogger
import zio.test.environment.TestClock

import scala.jdk.CollectionConverters._

object PollingFetcherTest extends DefaultRunnableSpec {

  val loggingEnv = Slf4jLogger.make((_, logEntry) => logEntry, Some("PollingFetcherTest"))

  override def spec =
    suite("PollingFetcher")(
      testM("immediately emits all records that were fetched in the first call in one Chunk") {
        val batchSize = 10
        val nrBatches = 1
        val records   = (0 until nrBatches * batchSize).map { i =>
          Record
            .builder()
            .data(SdkBytes.fromString("test", Charset.defaultCharset()))
            .partitionKey(s"key${i}")
            .sequenceNumber(s"${i}")
            .build()
        }

        (for {
          chunksFib <- PollingFetcher
                         .make("my-stream-1", FetchMode.Polling(10), _ => UIO.unit)
                         .use { fetcher =>
                           fetcher
                             .shardRecordStream("shard1", ShardIteratorType.TrimHorizon)
                             .mapChunks(Chunk.single)
                             .take(nrBatches)
                             .runCollect

                         }
                         .fork
          chunks    <- chunksFib.join
        } yield assert(chunks.headOption)(isSome(hasSize(equalTo(batchSize)))))
          .provideSomeLayer[ZTestEnv with Clock with Logging](ZLayer.succeed(mockClient(records)))
      },
      testM("immediately polls again when there are more records available") {
        val batchSize = 10
        val nrBatches = 5

        val records = (0 until nrBatches * batchSize).map { i =>
          Record
            .builder()
            .data(SdkBytes.fromString("test", Charset.defaultCharset()))
            .partitionKey(s"key${i}")
            .sequenceNumber(s"${i}")
            .build()
        }

        (for {
          chunksFib <- PollingFetcher
                         .make("my-stream-1", FetchMode.Polling(batchSize), _ => UIO.unit)
                         .use { fetcher =>
                           fetcher
                             .shardRecordStream("shard1", ShardIteratorType.TrimHorizon)
                             .mapChunks(Chunk.single)
                             .take(nrBatches)
                             .runCollect

                         }
                         .fork
          chunks    <- chunksFib.join
        } yield assertCompletes // The fact that we don't have to adjust our test clock suffices
        ).provideSomeLayer[ZTestEnv with Clock with Logging](ZLayer.succeed(mockClient(records)))
      }
    ).provideCustomLayer(loggingEnv ++ TestClock.default)

  // Simple single-shard GetRecords mock that uses the sequence number as shard iterator
  def mockClient(records: Seq[Record]): Client.Service =
    new StubClient {
      override def getShardIterator(
        streamName: String,
        shardId: String,
        iteratorType: Client.ShardIteratorType
      ): Task[String] = Task.succeed("0")

      override def getRecords(shardIterator: String, limit: Int): Task[GetRecordsResponse] = {
        val offset            = shardIterator.toInt
        val lastRecordOffset  = offset + limit
        val recordsInResponse = records.slice(offset, offset + limit)
        if (recordsInResponse.isEmpty) {
          println(s"Getting records from offset ${offset} for nr ${limit}, but only got ${records.size}")
          ZIO.never
        } else
          Task(
            GetRecordsResponse
              .builder()
              .records(recordsInResponse.asJava)
              .millisBehindLatest(if (lastRecordOffset == records.size) 0 else records.size - lastRecordOffset)
              .nextShardIterator(lastRecordOffset.toString)
              .build()
          )
      }
    }

  class StubClient extends Client.Service {

    import software.amazon.awssdk.services.kinesis.model.{
      Consumer,
      ConsumerDescription,
      GetRecordsResponse,
      PutRecordResponse,
      PutRecordsRequestEntry,
      PutRecordsResponse,
      Record,
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
//
//  class InMemoryClient(records: Ref[Map[String, Map[Shard, Seq[Record]]]]) extends StubClient {
//    override def getRecords( shardIterator: String, limit: Int ): Task[GetRecordsResponse] = ???
//
//    override def putRecord[R, T](streamName:  String, serializer:  Serializer[R, T], r:  ProducerRecord[T]): ZIO[R, Throwable, PutRecordResponse] ={
//      for {
//        rs <- records.get
//}
//      }
//
//
//
//
//  }
//
//  object InMemoryClient {
//    def makeRecord(value: SdkBytes) = Record.builder().data(value).
//  }
}
