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

object NativeConsumerTest extends DefaultRunnableSpec {
  /*
  - It must retrieve records from all shards
  - It must retrieve records in an expected order
  - Support both polling and enhanced fanout
  - Must restart from the given initial start point if no lease yet
  - Must restart from the record after the last checkpointed record for each shard
  - Must checkpoint the last staged checkpoint before shutdown
  - Correctly deserialize the records
  - TODO something about rate limits: maybe if we have multiple consumers active and run into rate limits?
   */

  val streamPrefix = ju.UUID.randomUUID().toString().take(6)

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
                .flatMapPar(Int.MaxValue) {
                  case (shard @ _, shardStream, checkpointer @ _) =>
                    shardStream
                }
                .take(nrRecords.toLong)
                .runCollect
            shardIds <- ZIO.service[AdminClient].flatMap(_.describeStream(streamName)).map(_.shards.map(_.shardId()))
            // _ <- produceR

          } yield assert(records.map(_.shardId).toSet)(equalTo(shardIds.toSet))
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
