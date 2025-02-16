package nl.vroste.zio.kinesis.client.zionative

import nl.vroste.zio.kinesis.client.zionative.DiagnosticEvent.PollComplete
import nl.vroste.zio.kinesis.client.zionative.FetchMode.Polling
import nl.vroste.zio.kinesis.client.zionative.fetcher.PollingFetcher
import software.amazon.awssdk.services.kinesis.model.{
  ExpiredIteratorException,
  ProvisionedThroughputExceededException
}
import zio._
import zio.aws.core.AwsError
import zio.aws.core.aspects.AwsCallAspect
import zio.aws.kinesis.model.primitives._
import zio.aws.kinesis.model.{
  ChildShard,
  GetRecordsResponse,
  GetShardIteratorResponse,
  HashKeyRange,
  Record,
  ShardIteratorType,
  StartingPosition
}
import zio.aws.kinesis.{ model, Kinesis }
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test._

import java.nio.charset.Charset

object PollingFetcherTest extends ZIOSpecDefault {

  /**
   * PollingFetcher must:
   *   - [X] immediately emit all records that were fetched in the first call in one Chunk
   *   - [X] immediately poll again when there are more records available
   *   - [X] delay polling when there are no more records available
   *   - [X] make no more than 5 calls per second per shard to GetRecords
   *   - [X] retry after some time on being throttled
   *   - [X] make the next call with the previous response's nextShardIterator
   *   - [X] end the shard stream when the shard has ended
   *   - [X] emit a diagnostic event for every completed poll
   *   - [X] refresh an iterator if it expires
   * @return
   */
  override def spec =
    suite("PollingFetcher")(
      test("immediately emits all records that were fetched in the first call in one Chunk") {
        val batchSize = 10
        val nrBatches = 1L
        val records   = makeRecords(nrBatches * batchSize)

        (for {
          chunksFib <- PollingFetcher
                         .make(StreamName(StreamName("my-stream-1")), FetchMode.Polling(10), _ => ZIO.unit)
                         .flatMap { fetcher =>
                           fetcher
                             .shardRecordStream(
                               ShardId(ShardId("shard1")),
                               StartingPosition(ShardIteratorType.TRIM_HORIZON)
                             )
                             .mapChunks(Chunk.single)
                             .take(nrBatches)
                             .runCollect

                         }
                         .fork
          chunks    <- chunksFib.join
        } yield assert(chunks.headOption)(isSome(hasSize(equalTo(batchSize)))))
          .provideSomeLayer[Scope](ZLayer.fromZIO(stubClient(records)))
      },
      test("immediately polls again when there are more records available") {
        val batchSize = 10
        val nrBatches = 5L

        val records = makeRecords(nrBatches * batchSize)

        (for {
            chunksFib <- PollingFetcher
                           .make(StreamName("my-stream-1"), FetchMode.Polling(batchSize), _ => ZIO.unit)
                           .flatMap { fetcher =>
                             fetcher
                               .shardRecordStream(ShardId("shard1"), StartingPosition(ShardIteratorType.TRIM_HORIZON))
                               .mapChunks(Chunk.single)
                               .take(nrBatches)
                               .runDrain
                           }
                           .fork
            _         <- chunksFib.join
          } yield assertCompletes // The fact that we don't have to adjust our test clock suffices
        ).provideSomeLayer[Scope](ZLayer.fromZIO(stubClient(records)))
      },
      test("delay polling when there are no more records available") {
        val batchSize    = 10
        val nrBatches    = 2L
        val pollInterval = 1.second

        val records = makeRecords(nrBatches * batchSize)

        (for {
          chunksReceived            <- Ref.make[Long](0)
          chunksFib                 <- PollingFetcher
                                         .make(
                                           StreamName("my-stream-1"),
                                           FetchMode.Polling(batchSize, Polling.dynamicSchedule(pollInterval)),
                                           _ => ZIO.unit
                                         )
                                         .flatMap { fetcher =>
                                           fetcher
                                             .shardRecordStream(ShardId("shard1"), StartingPosition(ShardIteratorType.TRIM_HORIZON))
                                             .mapChunks(Chunk.single)
                                             .tap(_ => chunksReceived.update(_ + 1))
                                             .take(nrBatches + 1)
                                             .runDrain
                                         }
                                         .fork
          _                         <- TestClock.adjust(0.seconds)
          _                          = println("Checking 1")
          chunksReceivedImmediately <- chunksReceived.get
          _                         <- TestClock.adjust(pollInterval)
          chunksReceivedLater       <- chunksReceived.get
          _                         <- chunksFib.join
        } yield assert(chunksReceivedImmediately)(equalTo(nrBatches)) && assert(chunksReceivedLater)(
          equalTo(nrBatches + 1)
        )).provideSomeLayer[Scope](ZLayer.fromZIO(stubClient(records)))
      },
      test("make no more than 5 calls per second per shard to GetRecords") {
        val batchSize    = 10
        val nrBatches    = 6L // More than 5, the GetRecords limit
        val pollInterval = 1.second

        val records = makeRecords(nrBatches * batchSize)

        (for {
          chunksReceived            <- Ref.make[Long](0)
          chunksFib                 <- PollingFetcher
                                         .make(
                                           StreamName("my-stream-1"),
                                           FetchMode.Polling(batchSize, Polling.dynamicSchedule(pollInterval)),
                                           _ => ZIO.unit
                                         )
                                         .flatMap { fetcher =>
                                           fetcher
                                             .shardRecordStream(ShardId("shard1"), StartingPosition(ShardIteratorType.TRIM_HORIZON))
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
        )).provideSomeLayer[Scope](ZLayer.fromZIO(stubClient(records)))
      },
      test("make the next call with the previous response's nextShardIterator") {
        val batchSize = 10
        val nrBatches = 2L

        val records = makeRecords(nrBatches * batchSize)

        (for {
          fetched      <- PollingFetcher
                            .make(StreamName("my-stream-1"), FetchMode.Polling(batchSize), _ => ZIO.unit)
                            .flatMap { fetcher =>
                              fetcher
                                .shardRecordStream(ShardId("shard1"), StartingPosition(ShardIteratorType.TRIM_HORIZON))
                                .mapChunks(Chunk.single)
                                .take(nrBatches)
                                .flattenChunks
                                .runCollect
                            }
          partitionKeys = fetched.map(_.partitionKey)
        } yield assert(partitionKeys)(equalTo(records.map(_.partitionKey))))
          .provideSomeLayer[Scope](ZLayer.fromZIO(stubClient(records)))
      },
      test("end the shard stream when the shard has ended") {
        val batchSize = 10
        val nrBatches = 3L
        val records   = makeRecords(nrBatches * batchSize)

        (for {
          _ <- PollingFetcher
                 .make(StreamName("my-stream-1"), FetchMode.Polling(batchSize), _ => ZIO.unit)
                 .flatMap { fetcher =>
                   fetcher
                     .shardRecordStream(ShardId("shard1"), StartingPosition(ShardIteratorType.TRIM_HORIZON))
                     .mapChunks(Chunk.single)
                     .catchAll {
                       case Left(e)  => ZStream.fail(e)
                       case Right(_) => ZStream.empty
                     }
                     .runCollect
                 }
        } yield assertCompletes)
          .provideSomeLayer[Scope](
            ZLayer.fromZIO(stubClient(records, endAfterRecords = true))
          )
      },
      test("emit a diagnostic event for every poll") {
        val batchSize = 10
        val nrBatches = 3L
        val records   = makeRecords(nrBatches * batchSize)

        (for {
          events        <- Ref.make[Seq[DiagnosticEvent]](Seq.empty)
          emitDiagnostic = (e: DiagnosticEvent) => events.update(_ :+ e)

          _             <- PollingFetcher
                             .make(StreamName("my-stream-1"), FetchMode.Polling(batchSize), emitDiagnostic)
                             .flatMap { fetcher =>
                               fetcher
                                 .shardRecordStream(ShardId("shard1"), StartingPosition(ShardIteratorType.TRIM_HORIZON))
                                 .mapChunks(Chunk.single)
                                 .take(nrBatches)
                                 .runCollect
                             }
          emittedEvents <- events.get
        } yield assert(emittedEvents)(forall(isSubtype[PollComplete](anything))))
          .provideSomeLayer[Scope](ZLayer.fromZIO(stubClient(records)))
      },
      test("retry after some time when throttled") {
        val batchSize    = 10
        val nrBatches    = 3L
        val pollInterval = 1.second

        val records = makeRecords(nrBatches * batchSize)

        (for {
          chunksReceived            <- Ref.make[Long](0)
          requestNr                 <- Ref.make[Int](0)
          doThrottle                 = (_: String, _: Int) => requestNr.getAndUpdate(_ + 1).map(_ == 1)
          chunksFib                 <- PollingFetcher
                                         .make(
                                           StreamName("my-stream-1"),
                                           FetchMode.Polling(batchSize, Polling.dynamicSchedule(pollInterval)),
                                           _ => ZIO.unit
                                         )
                                         .flatMap { fetcher =>
                                           fetcher
                                             .shardRecordStream(ShardId("shard1"), StartingPosition(ShardIteratorType.TRIM_HORIZON))
                                             .mapChunks(Chunk.single)
                                             .tap(_ => chunksReceived.update(_ + 1))
                                             .take(nrBatches)
                                             .runDrain
                                         }
                                         .provideSomeLayer[Scope](
                                           ZLayer.fromZIO(stubClient(records, doThrottle = doThrottle))
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
      },
      test("restart from the correct sequence number when an iterator has expired") {
        val batchSize    = 10
        val nrBatches    = 3L
        val pollInterval = 1.second

        val records = makeRecords(nrBatches * batchSize)

        val doExpire = (next: Int, count: Int) => ZIO.succeed(next == batchSize && count == 1)
        for {
          fetcherFib <-
            PollingFetcher
              .make("my-stream-1", FetchMode.Polling(batchSize, Polling.dynamicSchedule(pollInterval)), _ => ZIO.unit)
              .flatMap { fetcher =>
                fetcher
                  .shardRecordStream(ShardId("shard1"), StartingPosition(ShardIteratorType.TRIM_HORIZON))
                  .mapChunks(Chunk.single)
                  .take(nrBatches)
                  .runCollect
              }
              .provideSomeLayer[Scope](
                ZLayer.fromZIO(stubClient(records, doExpire = doExpire))
              )
              .fork
          _          <- TestClock.adjust(0.seconds)

          received <- fetcherFib.join
        } yield assert(received)(hasSize(equalTo(nrBatches.toInt))) && assertTrue(
          received.flatten.map(_.partitionKey) == Chunk.fromIterable(records.map(_.partitionKey))
        )
      }
    ) @@ TestAspect.timeout(30.seconds)

  private def makeRecords(nrRecords: Long): Seq[Record] =
    (0 until nrRecords.toInt).map { i =>
      Record(
        SequenceNumber(s"${i}"),
        data = Data(Chunk.fromByteBuffer(Charset.defaultCharset().encode("test"))),
        partitionKey = PartitionKey(s"key${i}")
      )
    }

// Simple single-shard GetRecords mock that uses the sequence number as shard iterator
  private def stubClient(
    records: Seq[Record],
    endAfterRecords: Boolean = false,
    doThrottle: (String, Int) => UIO[Boolean] = (_, _) => ZIO.succeed(false),
    doExpire: (Int, Int) => UIO[Boolean] = (_, _) => ZIO.succeed(false)
  ): UIO[Kinesis] =
    Ref.make[Map[Int, Int]](Map.empty.withDefaultValue(0)).map { issuedIterators =>
      new StubClient { self =>
        private def issueIterator(sequenceNumber: Int): UIO[String] =
          issuedIterators.modify { old =>
            (sequenceNumber.toString(), old + (sequenceNumber -> (old(sequenceNumber) + 1)))
          }

        override def withAspect[R](newAspect: AwsCallAspect[R], r: ZEnvironment[R]): Kinesis = self

        override def getShardIterator(
          request: model.GetShardIteratorRequest
        ): IO[AwsError, GetShardIteratorResponse.ReadOnly] = {
          val baseSequenceNumber = request.startingSequenceNumber.map(_.toInt).getOrElse(0)
          val sequenceNumber     =
            if (request.shardIteratorType == ShardIteratorType.AFTER_SEQUENCE_NUMBER)
              baseSequenceNumber + 1
            else
              baseSequenceNumber
          issueIterator(sequenceNumber)
            .map(iterator => GetShardIteratorResponse(Some(ShardIterator(iterator))).asReadOnly)
        }

        override def getRecords(request: model.GetRecordsRequest): IO[AwsError, GetRecordsResponse.ReadOnly] = {
          val shardIterator = request.shardIterator
          val limit         = request.limit.getOrElse(0)
          val offset        = shardIterator.toInt
          val expire        = issuedIterators.get.flatMap[Any, Nothing, Boolean](issued => doExpire(offset, issued(offset)))

          doThrottle(shardIterator, limit).zip(expire).flatMap { case (throttle, expire) =>
            if (throttle)
              ZIO.fail(
                AwsError.fromThrowable(ProvisionedThroughputExceededException.builder.message("take it easy").build())
              )
            else if (expire)
              ZIO.fail(AwsError.fromThrowable(ExpiredIteratorException.builder().message("too late").build()))
            else {
              val lastRecordOffset   = offset + limit
              val recordsInResponse  = records.slice(offset, offset + limit)
              val shouldEnd          = lastRecordOffset >= records.size && endAfterRecords
              val childShards        =
                if (shouldEnd)
                  Some(
                    Seq(
                      ChildShard(
                        ShardId("shard-002"),
                        Seq(ShardId("001")),
                        HashKeyRange(HashKey("123"), HashKey("456"))
                      )
                    )
                  )
                else None
              val millisBehindLatest = if (lastRecordOffset >= records.size) 0 else records.size - lastRecordOffset

              val nextShardIterator = if (shouldEnd) ZIO.succeed(null) else issueIterator(lastRecordOffset)

              //          println(s"GetRecords from ${shardIterator} (max ${limit}. Next iterator: ${nextShardIterator}")
              nextShardIterator.map { iterator =>
                GetRecordsResponse(
                  recordsInResponse,
                  Some(ShardIterator(iterator)),
                  Some(MillisBehindLatest(millisBehindLatest.toLong)),
                  childShards
                ).asReadOnly
              }
            }
          }
        }
      }
    }
}
