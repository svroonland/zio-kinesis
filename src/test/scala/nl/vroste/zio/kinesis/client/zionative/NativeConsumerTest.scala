package nl.vroste.zio.kinesis.client.zionative

import zio.test._
import zio.ZIO
import zio.Has
import nl.vroste.zio.kinesis.client.Client
import nl.vroste.zio.kinesis.client.AdminClient
import java.{ util => ju }
import nl.vroste.zio.kinesis.client.Producer
import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import zio.stream.ZStream
import nl.vroste.zio.kinesis.client.serde.Serde
import zio.clock.Clock
import zio.console._
import zio.duration._
import nl.vroste.zio.kinesis.client.TestUtil.retryOnResourceNotFound
import zio.Chunk
import nl.vroste.zio.kinesis.client.zionative.Consumer
import nl.vroste.zio.kinesis.client.TestUtil.Layers
import zio.ZManaged
import zio.test.Assertion._
import zio.stream.ZTransducer
import zio.Schedule

object NativeConsumerTest extends DefaultRunnableSpec {
  /*
  - It must retrieve records from all shards
  - Support both polling and enhanced fanout
  - Must restart from the given initial start point if no lease yet
  - Must restart from the record after the last checkpointed record for each shard
  - Must checkpoint the last staged checkpoint before shutdown
  - Correctly deserialize the records
  - TODO something about rate limits: maybe if we have multiple consumers active and run into rate limits?
   */

  def streamPrefix = ju.UUID.randomUUID().toString().take(6)

  override def spec =
    suite("ZIO Native Kinesis Stream Consumer")(
      testM("retrieve records from all shards") {
        val streamName = streamPrefix + "testStream"
        val nrRecords  = 2000
        val nrShards   = 5

        withStream(streamName, shards = nrShards) {
          for {
            _        <- produceSampleRecords(streamName, nrRecords).fork
            records  <-
              Consumer
                .shardedStream(streamName, "test1", Serde.asciiString, fetchMode = FetchMode.Polling(batchSize = 1000))
                .flatMapPar(Int.MaxValue)(_._2)
                .take(nrRecords.toLong)
                .runCollect
            shardIds <- ZIO.service[AdminClient].flatMap(_.describeStream(streamName)).map(_.shards.map(_.shardId()))

          } yield assert(records.map(_.shardId).toSet)(equalTo(shardIds.toSet))
        }
      } @@ TestAspect.ignore,
      testM("continue from the next message after the last checkpoint") {
        val streamName      = streamPrefix + "testStream-2"
        val applicationName = streamPrefix + "test2"
        val nrRecords       = 2000
        val nrShards        = 5

        withStream(streamName, shards = nrShards) {
          for {
            _            <- produceSampleRecords(streamName, nrRecords).fork
            // Take the first 1000 records
            recordsPart1 <- Consumer
                              .shardedStream(
                                streamName,
                                applicationName,
                                Serde.asciiString,
                                fetchMode = FetchMode.Polling(batchSize = 1000)
                              )
                              .flatMapPar(Int.MaxValue) {
                                case (shard @ _, shardStream, checkpointer) =>
                                  shardStream
                                    .tap(checkpointer.stage) // It will automatically checkpoint at stream end
                              }
                              .take((nrRecords / 2).toLong)
                              .runCollect

            // Consume the rest with the same app
            _            <- putStrLn("Starting second consumer")
            recordsPart2 <- Consumer
                              .shardedStream(
                                streamName,
                                applicationName,
                                Serde.asciiString,
                                fetchMode = FetchMode.Polling(batchSize = 1000)
                              )
                              .flatMapPar(Int.MaxValue) {
                                case (shard @ _, shardStream, checkpointer) =>
                                  shardStream
                                    .tap(checkpointer.stage) // It will automatically checkpoint at stream end
                              }
                              .take((nrRecords / 2).toLong + 50)
                              .timeout(2.seconds)
                              .runCollect

          } yield assert(recordsPart2.size)(equalTo(nrRecords / 2)) &&
            assert(recordsPart1.map(_.sequenceNumber).toSet)(not(equalTo(recordsPart2.map(_.sequenceNumber).toSet)))
        }
      }
    ).provideSomeLayer(
      (Layers.kinesisAsyncClient >>> (Layers.adminClient ++ Layers.client)).orDie ++ zio.test.environment.testEnvironment ++ Clock.live ++ Layers.dynamo.orDie
    )

  def withStream[R, A](name: String, shards: Int)(f: ZIO[R, Throwable, A]): ZIO[Has[AdminClient] with R, Throwable, A] =
    (for {
      client <- ZManaged.service[AdminClient]
      _      <- client.createStream(name, shards).toManaged(_ => client.deleteStream(name).orDie)
    } yield ()).use_(f)

  def produceSampleRecords(
    streamName: String,
    nrRecords: Int,
    chunkSize: Int = 100
  ): ZIO[Has[Client] with Clock, Throwable, Unit] =
    (for {
      client   <- ZIO.service[Client].toManaged_
      producer <- Producer.make(streamName, client, Serde.asciiString)
    } yield producer).use { producer =>
      val records =
        (1 to nrRecords).map(i => ProducerRecord(s"key$i", s"msg$i"))
      ZStream
        .fromIterable(records)
        .chunkN(chunkSize)
        .mapChunksM(
          producer
            .produceChunk(_)
            .tapError(e => putStrLn(s"error: $e").provideLayer(Console.live))
            .retry(retryOnResourceNotFound)
            .fork
            .map(fib => Chunk.single(fib))
          // .tap(_ => ZIO.sleep(1.second))
        )
        .mapMPar(24)(_.join)
        .runDrain
    }

}
