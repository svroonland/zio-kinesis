package nl.vroste.zio.kinesis.client.zionative
import java.nio.charset.Charset

import nl.vroste.zio.kinesis.client.Client
import nl.vroste.zio.kinesis.client.Client.ShardIteratorType
import nl.vroste.zio.kinesis.client.zionative.FetchMode.Polling
import nl.vroste.zio.kinesis.client.zionative.fetcher.PollingFetcher
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.model.{ GetRecordsResponse, Record }
import zio._
import zio.clock.Clock
import zio.logging.Logging
import zio.logging.slf4j.Slf4jLogger
import zio.test.Assertion._
import zio.test._
import zio.test.environment.TestClock
import zio.duration._

import scala.jdk.CollectionConverters._

object PollingFetcherTest extends DefaultRunnableSpec {

  val loggingEnv = Slf4jLogger.make((_, logEntry) => logEntry, Some("PollingFetcherTest"))

  /**
   * PollingFetcher must:
   * - [X] immediately emit all records that were fetched in the first call in one Chunk
   * - [X] immediately poll again when there are more records available
   * - [X] delay polling when there are no more records available
   * - [ ] make no more than 5 calls per second to GetShardIterator
   * - [X] make no more than 5 calls per second per shard to GetRecords
   * - [ ] retry after some time on being throttled
   * - [X] make the next call with the previous response's nextShardIterator
   * - [X] end the shard stream when the shard has ended
   * - [ ] emit a diagnostic event for every completed poll
   *
   * @return
   */
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
      },
      testM("delay polling when there are no more records available") {
        val batchSize    = 10
        val nrBatches    = 2
        val pollInterval = 1.second

        val records = (0 until nrBatches * batchSize).map { i =>
          Record
            .builder()
            .data(SdkBytes.fromString("test", Charset.defaultCharset()))
            .partitionKey(s"key${i}")
            .sequenceNumber(s"${i}")
            .build()
        }

        (for {
          chunksReceived            <- Ref.make[Int](0)
          chunksFib                 <-
            PollingFetcher
              .make("my-stream-1", FetchMode.Polling(batchSize, Polling.dynamicSchedule(pollInterval)), _ => UIO.unit)
              .use { fetcher =>
                fetcher
                  .shardRecordStream("shard1", ShardIteratorType.TrimHorizon)
                  .mapChunks(Chunk.single)
                  .tap(_ => chunksReceived.update(_ + 1))
                  .take(nrBatches + 1)
                  .runDrain
              }
              .fork
          _                         <- TestClock.adjust(0.seconds)
          chunksReceivedImmediately <- chunksReceived.get
          _                         <- TestClock.adjust(pollInterval)
          chunksReceivedLater       <- chunksReceived.get
          _                         <- chunksFib.join
        } yield assert(chunksReceivedImmediately)(equalTo(nrBatches)) && assert(chunksReceivedLater)(
          equalTo(nrBatches + 1)
        )).provideSomeLayer[ZTestEnv with Clock with Logging](ZLayer.succeed(mockClient(records)))
      },
      testM("make no more than 5 calls per second per shard to GetRecords") {
        val batchSize    = 10
        val nrBatches    = 6 // More than 5, the GetRecords limit
        val pollInterval = 1.second

        val records = (0 until nrBatches * batchSize).map { i =>
          Record
            .builder()
            .data(SdkBytes.fromString("test", Charset.defaultCharset()))
            .partitionKey(s"key${i}")
            .sequenceNumber(s"${i}")
            .build()
        }

        (for {
          chunksReceived            <- Ref.make[Int](0)
          chunksFib                 <-
            PollingFetcher
              .make("my-stream-1", FetchMode.Polling(batchSize, Polling.dynamicSchedule(pollInterval)), _ => UIO.unit)
              .use { fetcher =>
                fetcher
                  .shardRecordStream("shard1", ShardIteratorType.TrimHorizon)
                  .mapChunks(Chunk.single)
                  .tap(_ => chunksReceived.update(_ + 1))
                  .take(nrBatches)
                  .runDrain
              }
              .fork
          _                         <- TestClock.adjust(0.seconds)
          chunksReceivedImmediately <- chunksReceived.get
          _                         <- TestClock.adjust(pollInterval)
          chunksReceivedLater       <- chunksReceived.get
          _                         <- chunksFib.join
        } yield assert(chunksReceivedImmediately)(equalTo(5)) && assert(chunksReceivedLater)(
          equalTo(nrBatches)
        )).provideSomeLayer[ZTestEnv with Clock with Logging](ZLayer.succeed(mockClient(records)))
      },
      testM("make the next call with the previous response's nextShardIterator") {
        val batchSize = 10
        val nrBatches = 2

        val records = (0 until nrBatches * batchSize).map { i =>
          Record
            .builder()
            .data(SdkBytes.fromString("test", Charset.defaultCharset()))
            .partitionKey(s"key${i}")
            .sequenceNumber(s"${i}")
            .build()
        }

        (for {
          fetched      <- PollingFetcher
                       .make("my-stream-1", FetchMode.Polling(batchSize), _ => UIO.unit)
                       .use { fetcher =>
                         fetcher
                           .shardRecordStream("shard1", ShardIteratorType.TrimHorizon)
                           .mapChunks(Chunk.single)
                           .take(nrBatches)
                           .flattenChunks
                           .runCollect
                       }
          partitionKeys = fetched.map(_.partitionKey)
        } yield assert(partitionKeys)(equalTo(records.map(_.partitionKey))))
          .provideSomeLayer[ZTestEnv with Clock with Logging](ZLayer.succeed(mockClient(records)))
      },
      testM("end the shard stream when the shard has ended") {
        val batchSize = 10
        val nrBatches = 3
        val records   = (0 until nrBatches * batchSize).map { i =>
          Record
            .builder()
            .data(SdkBytes.fromString("test", Charset.defaultCharset()))
            .partitionKey(s"key${i}")
            .sequenceNumber(s"${i}")
            .build()
        }

        (for {
          _ <- PollingFetcher
                 .make("my-stream-1", FetchMode.Polling(batchSize), _ => UIO.unit)
                 .use { fetcher =>
                   fetcher
                     .shardRecordStream("shard1", ShardIteratorType.TrimHorizon)
                     .mapChunks(Chunk.single)
                     .runCollect
                 }
        } yield assertCompletes)
          .provideSomeLayer[ZTestEnv with Clock with Logging](
            ZLayer.succeed(mockClient(records, endAfterRecords = true))
          )
      }
    ).provideCustomLayer(loggingEnv ++ TestClock.default)

  // Simple single-shard GetRecords mock that uses the sequence number as shard iterator
  def mockClient(records: Seq[Record], endAfterRecords: Boolean = false): Client.Service =
    new StubClient {
      override def getShardIterator(
        streamName: String,
        shardId: String,
        iteratorType: Client.ShardIteratorType
      ): Task[String] = Task.succeed("0")

      override def getRecords(shardIterator: String, limit: Int): Task[GetRecordsResponse] =
        Task {
          val offset             = shardIterator.toInt
          val lastRecordOffset   = offset + limit
          val recordsInResponse  = records.slice(offset, offset + limit)
          val nextShardIterator  =
            if (lastRecordOffset >= records.size && endAfterRecords) null else lastRecordOffset.toString
          val millisBehindLatest = if (lastRecordOffset >= records.size) 0 else records.size - lastRecordOffset

          println(s"GetRecords from ${shardIterator} (max ${limit}. Next iterator: ${nextShardIterator}")

          GetRecordsResponse
            .builder()
            .records(recordsInResponse.asJava)
            .millisBehindLatest(millisBehindLatest)
            .nextShardIterator(nextShardIterator)
            .build()
        }
    }
}
