package nl.vroste.zio.kinesis.client.zionative
import java.nio.charset.Charset

import io.github.vigoo.zioaws.core.AwsError
import io.github.vigoo.zioaws.kinesis.{ model, Kinesis }
import io.github.vigoo.zioaws.kinesis.model.{
  GetRecordsResponse,
  GetShardIteratorResponse,
  Record,
  ShardIteratorType,
  StartingPosition
}
import nl.vroste.zio.kinesis.client.zionative.DiagnosticEvent.PollComplete
import nl.vroste.zio.kinesis.client.zionative.FetchMode.Polling
import nl.vroste.zio.kinesis.client.zionative.fetcher.PollingFetcher
import software.amazon.awssdk.services.kinesis.{ model => aws }
import aws.ChildShard
import aws.HashKeyRange
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException
import zio._
import zio.logging.Logging
import zio.logging.slf4j.Slf4jLogger
import zio.test.Assertion._
import zio.test._
import zio.test.environment.TestClock
import zio.duration._
import zio.stream.ZStream

import scala.jdk.CollectionConverters._

object PollingFetcherTest extends DefaultRunnableSpec {

  val loggingLayer = Slf4jLogger.make((_, logEntry) => logEntry, Some("PollingFetcherTest"))

  /**
   * PollingFetcher must:
   * - [X] immediately emit all records that were fetched in the first call in one Chunk
   * - [X] immediately poll again when there are more records available
   * - [X] delay polling when there are no more records available
   * - [X] make no more than 5 calls per second per shard to GetRecords
   * - [X] retry after some time on being throttled
   * - [X] make the next call with the previous response's nextShardIterator
   * - [X] end the shard stream when the shard has ended
   * - [X] emit a diagnostic event for every completed poll
   *
   * @return
   */
  override def spec =
    suite("PollingFetcher")(
      testM("immediately emits all records that were fetched in the first call in one Chunk") {
        val batchSize = 10
        val nrBatches = 1L
        val records   = makeRecords(nrBatches * batchSize)

        (for {
          chunksFib <- PollingFetcher
                         .make("my-stream-1", FetchMode.Polling(10), _ => UIO.unit)
                         .use { fetcher =>
                           fetcher
                             .shardRecordStream("shard1", StartingPosition(ShardIteratorType.TRIM_HORIZON))
                             .mapChunks(Chunk.single)
                             .take(nrBatches)
                             .runCollect

                         }
                         .fork
          chunks    <- chunksFib.join
        } yield assert(chunks.headOption)(isSome(hasSize(equalTo(batchSize)))))
          .provideSomeLayer[ZEnv with Logging](ZLayer.succeed(stubClient(records)))
      },
      testM("immediately polls again when there are more records available") {
        val batchSize = 10
        val nrBatches = 5L

        val records = makeRecords(nrBatches * batchSize)

        (for {
          chunksFib <- PollingFetcher
                         .make("my-stream-1", FetchMode.Polling(batchSize), _ => UIO.unit)
                         .use { fetcher =>
                           fetcher
                             .shardRecordStream("shard1", StartingPosition(ShardIteratorType.TRIM_HORIZON))
                             .mapChunks(Chunk.single)
                             .take(nrBatches)
                             .runDrain
                         }
                         .fork
          _         <- chunksFib.join
        } yield assertCompletes // The fact that we don't have to adjust our test clock suffices
        ).provideSomeLayer[ZEnv with Logging](ZLayer.succeed(stubClient(records)))
      },
      testM("delay polling when there are no more records available") {
        val batchSize    = 10
        val nrBatches    = 2L
        val pollInterval = 1.second

        val records = makeRecords(nrBatches * batchSize)

        (for {
          chunksReceived            <- Ref.make[Long](0)
          chunksFib                 <-
            PollingFetcher
              .make("my-stream-1", FetchMode.Polling(batchSize, Polling.dynamicSchedule(pollInterval)), _ => UIO.unit)
              .use { fetcher =>
                fetcher
                  .shardRecordStream("shard1", StartingPosition(ShardIteratorType.TRIM_HORIZON))
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
        )).provideSomeLayer[ZEnv with Logging with TestClock](ZLayer.succeed(stubClient(records)))
      },
      testM("make no more than 5 calls per second per shard to GetRecords") {
        val batchSize    = 10
        val nrBatches    = 6L // More than 5, the GetRecords limit
        val pollInterval = 1.second

        val records = makeRecords(nrBatches * batchSize)

        (for {
          chunksReceived            <- Ref.make[Long](0)
          chunksFib                 <-
            PollingFetcher
              .make("my-stream-1", FetchMode.Polling(batchSize, Polling.dynamicSchedule(pollInterval)), _ => UIO.unit)
              .use { fetcher =>
                fetcher
                  .shardRecordStream("shard1", StartingPosition(ShardIteratorType.TRIM_HORIZON))
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
        } yield assert(chunksReceivedImmediately)(equalTo(5L)) && assert(chunksReceivedLater)(
          equalTo(nrBatches)
        )).provideSomeLayer[ZEnv with Logging with TestClock](ZLayer.succeed(stubClient(records)))
      },
      testM("make the next call with the previous response's nextShardIterator") {
        val batchSize = 10
        val nrBatches = 2L

        val records = makeRecords(nrBatches * batchSize)

        (for {
          fetched      <- PollingFetcher
                       .make("my-stream-1", FetchMode.Polling(batchSize), _ => UIO.unit)
                       .use { fetcher =>
                         fetcher
                           .shardRecordStream("shard1", StartingPosition(ShardIteratorType.TRIM_HORIZON))
                           .mapChunks(Chunk.single)
                           .take(nrBatches)
                           .flattenChunks
                           .runCollect
                       }
          partitionKeys = fetched.map(_.partitionKey)
        } yield assert(partitionKeys)(equalTo(records.map(_.partitionKey))))
          .provideSomeLayer[ZEnv with Logging](ZLayer.succeed(stubClient(records)))
      },
      testM("end the shard stream when the shard has ended") {
        val batchSize = 10
        val nrBatches = 3L
        val records   = makeRecords(nrBatches * batchSize)

        (for {
          _ <- PollingFetcher
                 .make("my-stream-1", FetchMode.Polling(batchSize), _ => UIO.unit)
                 .use { fetcher =>
                   fetcher
                     .shardRecordStream("shard1", StartingPosition(ShardIteratorType.TRIM_HORIZON))
                     .mapChunks(Chunk.single)
                     .catchAll {
                       case Left(e)  => ZStream.fail(e)
                       case Right(_) => ZStream.empty
                     }
                     .runCollect
                 }
        } yield assertCompletes)
          .provideSomeLayer[ZEnv with Logging](
            ZLayer.succeed(stubClient(records, endAfterRecords = true))
          )
      },
      testM("emit a diagnostic event for every poll") {
        val batchSize = 10
        val nrBatches = 3L
        val records   = makeRecords(nrBatches * batchSize)

        (for {
          events        <- Ref.make[Seq[DiagnosticEvent]](Seq.empty)
          emitDiagnostic = (e: DiagnosticEvent) => events.update(_ :+ e)

          _             <- PollingFetcher
                 .make("my-stream-1", FetchMode.Polling(batchSize), emitDiagnostic)
                 .use { fetcher =>
                   fetcher
                     .shardRecordStream("shard1", StartingPosition(ShardIteratorType.TRIM_HORIZON))
                     .mapChunks(Chunk.single)
                     .take(nrBatches)
                     .runCollect
                 }
          emittedEvents <- events.get
        } yield assert(emittedEvents)(forall(isSubtype[PollComplete](anything))))
          .provideSomeLayer[ZEnv with Logging](ZLayer.succeed(stubClient(records)))
      },
      testM("retry after some time when throttled") {
        val batchSize    = 10
        val nrBatches    = 3L
        val pollInterval = 1.second

        val records = makeRecords(nrBatches * batchSize)

        (for {
          chunksReceived            <- Ref.make[Long](0)
          requestNr                 <- Ref.make[Int](0)
          doThrottle                 = (_: String, _: Int) => requestNr.getAndUpdate(_ + 1).map(_ == 1)
          chunksFib                 <-
            PollingFetcher
              .make("my-stream-1", FetchMode.Polling(batchSize, Polling.dynamicSchedule(pollInterval)), _ => UIO.unit)
              .use { fetcher =>
                fetcher
                  .shardRecordStream("shard1", StartingPosition(ShardIteratorType.TRIM_HORIZON))
                  .mapChunks(Chunk.single)
                  .tap(_ => chunksReceived.update(_ + 1))
                  .take(nrBatches)
                  .runDrain
              }
              .provideSomeLayer[ZEnv with Logging with TestClock](
                ZLayer.succeed(stubClient(records, doThrottle = doThrottle))
              )
              .fork
          _                         <- TestClock.adjust(0.seconds)
          chunksReceivedImmediately <- chunksReceived.get
          _                         <- TestClock.adjust(5.second)
          chunksReceivedLater       <- chunksReceived.get
          _                         <- chunksFib.join
        } yield assert(chunksReceivedImmediately)(equalTo(1L)) && assert(chunksReceivedLater)(
          equalTo(nrBatches)
        ))
      }
    ).provideCustomLayer(loggingLayer ++ TestClock.default)

  private def makeRecords(nrRecords: Long): Seq[Record] =
    (0 until nrRecords.toInt).map { i =>
      Record(s"${i}", data = Chunk.fromByteBuffer(Charset.defaultCharset().encode("test")), partitionKey = s"key${i}")
    }

// Simple single-shard GetRecords mock that uses the sequence number as shard iterator
  private def stubClient(
    records: Seq[Record],
    endAfterRecords: Boolean = false,
    doThrottle: (String, Int) => UIO[Boolean] = (_, _) => UIO(false)
  ): Kinesis.Service =
    new StubClient {
      override def getShardIterator(
        request: model.GetShardIteratorRequest
      ): IO[AwsError, GetShardIteratorResponse.ReadOnly] =
        IO.succeed(GetShardIteratorResponse.wrap(aws.GetShardIteratorResponse.builder().shardIterator("0").build()))

      override def getRecords(request: model.GetRecordsRequest): IO[AwsError, GetRecordsResponse.ReadOnly] = {
        val shardIterator = request.shardIterator
        val limit         = request.limit.getOrElse(0)

        doThrottle(shardIterator, limit).flatMap { throttle =>
          if (throttle)
            IO.fail(
              AwsError.fromThrowable(ProvisionedThroughputExceededException.builder.message("take it easy").build())
            )
          else {
            val offset             = shardIterator.toInt
            val lastRecordOffset   = offset + limit
            val recordsInResponse  = records.slice(offset, offset + limit)
            val shouldEnd          = lastRecordOffset >= records.size && endAfterRecords
            val nextShardIterator  =
              if (shouldEnd) null else lastRecordOffset.toString
            val childShards        =
              if (shouldEnd)
                Seq(
                  ChildShard
                    .builder()
                    .shardId("shard-002")
                    .parentShards("001")
                    .hashKeyRange(HashKeyRange.builder().startingHashKey("123").endingHashKey("456").build())
                    .build
                )
              else null
            val millisBehindLatest = if (lastRecordOffset >= records.size) 0 else records.size - lastRecordOffset

            //          println(s"GetRecords from ${shardIterator} (max ${limit}. Next iterator: ${nextShardIterator}")

            val awsResponse = aws.GetRecordsResponse
              .builder()
              .millisBehindLatest(millisBehindLatest)
              .nextShardIterator(nextShardIterator)
              .childShards(childShards.asJava)
              .records(recordsInResponse.map(_.buildAwsValue()).asJavaCollection)
              .build()

            IO.succeed(GetRecordsResponse.wrap(awsResponse))
          }
        }
      }
    }
}
