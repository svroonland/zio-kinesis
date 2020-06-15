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
import nl.vroste.zio.kinesis.client.TestUtil.retryOnResourceNotFound
import zio.Chunk
import nl.vroste.zio.kinesis.client.zionative.Consumer
import zio.duration._
import nl.vroste.zio.kinesis.client.TestUtil.Layers
import zio.ZManaged
import zio.test.Assertion._
import zio.UIO
import zio.Schedule
import zio.stream.ZTransducer
import zio.ZLayer
import zio.logging.log
import zio.logging.slf4j.Slf4jLogger
// import zio.logging.LogAnnotation

object NativeConsumerTest extends DefaultRunnableSpec {
  /*
  - It must retrieve records from all shards
  - Support both polling and enhanced fanout
  - Must restart from the given initial start point if no lease yet
  - Must restart from the record after the last checkpointed record for each shard
  - Should release leases at shutdown (another worker should aquicre all leases directly without having to steal)
  - Must checkpoint the last staged checkpoint before shutdown
  - Correctly deserialize the records
  - TODO something about rate limits: maybe if we have multiple consumers active and run into rate limits?
  - Two works must be able to start up concurrently (not sure what the assertion would be)
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
            _        <- produceSampleRecords(streamName, nrRecords, chunkSize = 200).fork
            records  <- Consumer
                         .shardedStream(
                           streamName,
                           s"${streamName}-test1",
                           Serde.asciiString,
                           fetchMode = FetchMode.Polling(batchSize = 1000)
                         )
                         .flatMapPar(Int.MaxValue)(_._2)
                         .tap(r => UIO(println(s"Got record on shard ${r.shardId}")))
                         .take(nrRecords.toLong)
                         .runCollect
            shardIds <- ZIO.service[AdminClient].flatMap(_.describeStream(streamName)).map(_.shards.map(_.shardId()))

          } yield assert(records.map(_.shardId).toSet)(equalTo(shardIds.toSet))
        }
      },
      testM("continue from the next message after the last checkpoint") {
        val streamName      = streamPrefix + "testStream-2"
        val applicationName = streamPrefix + "test2"
        val nrRecords       = 2000
        val nrShards        = 5

        withStream(streamName, shards = nrShards) {
          for {
            _            <- produceSampleRecords(streamName, nrRecords)
            // Take the first 1000 records
            recordsPart1 <- Consumer
                              .shardedStream(streamName, applicationName, Serde.asciiString)
                              .flatMapPar(Int.MaxValue) {
                                case (shard @ _, shardStream, checkpointer) =>
                                  shardStream.map((_, checkpointer))
                              }
                              .tap {
                                case (r, checkpointer) => checkpointer.stage(r)
                              } // It will automatically checkpoint at stream end
                              .map(_._1)
                              .take((nrRecords / 2).toLong)
                              .runCollect

            // Consume the rest with the same app
            _            <- putStrLn("Starting second consumer")
            recordsPart2 <- Consumer
                              .shardedStream(streamName, applicationName, Serde.asciiString)
                              .flatMapPar(Int.MaxValue) {
                                case (shard @ _, shardStream, checkpointer) => shardStream.map((_, checkpointer))
                              }
                              .tap {
                                case (r, checkpointer) => checkpointer.stage(r)
                              } // It will automatically checkpoint at stream end
                              .map(_._1)
                              .take((nrRecords / 2).toLong)
                              .runCollect

          } yield assert(recordsPart1.size)(equalTo(nrRecords / 2) ?? "records part 1 size") &&
            assert(recordsPart2.size)(equalTo(nrRecords / 2) ?? "records part 2 size") &&
            assert(recordsPart1.map(_.sequenceNumber).toSet)(
              hasNoneOf(recordsPart2.map(_.sequenceNumber).toSet) ?? "records pt1 not equal to pt2"
            )
        }
      },
      testM("worker steals leases from other worker until they both have an equal share") {
        val streamName      = streamPrefix + "testStream-3"
        val applicationName = streamPrefix + "test3"
        val nrRecords       = 20000
        val nrShards        = 5

        withStream(streamName, shards = nrShards) {
          for {
            _        <- produceSampleRecords(streamName, nrRecords, chunkSize = 10, throttle = Some(1.second)).fork
            consumer1 = Consumer
                          .shardedStream(streamName, applicationName, Serde.asciiString, workerId = "worker1")
                          .flatMapPar(Int.MaxValue) {
                            case (shard @ _, shardStream, checkpointer) =>
                              shardStream
                              // .tap(r => UIO(println(s"Worker 1 got record on shard ${r.shardId}")))
                                .tap(checkpointer.stage)
                                .aggregateAsyncWithin(ZTransducer.collectAllN(20), Schedule.fixed(1.second))
                                .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
                                .tap(_ =>
                                  log.info(s"Worker 1 checkpointing for shard ${shard}") *> checkpointer.checkpoint
                                )
                                .catchAll {
                                  case Right(ShardLeaseLost) =>
                                    ZStream.empty
                                  case Left(e)               => ZStream.fail(e)
                                }
                                .mapConcat(identity(_))
                          }
            consumer2 = Consumer
                          .shardedStream(streamName, applicationName, Serde.asciiString, workerId = "worker2")
                          .flatMapPar(Int.MaxValue) {
                            case (shard @ _, shardStream, checkpointer) =>
                              shardStream
                              // .tap(r => UIO(println(s"Worker 2 got record on shard ${r.shardId}")))
                                .tap(checkpointer.stage)
                                .aggregateAsyncWithin(ZTransducer.collectAllN(20), Schedule.fixed(1.second))
                                .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
                                .tap(_ =>
                                  UIO(println(s"Worker 2 checkpointing for shard ${shard}")) *> checkpointer.checkpoint
                                )
                                .catchAll {
                                  case Right(ShardLeaseLost) =>
                                    ZStream.empty
                                  case Left(e)               => ZStream.fail(e)
                                }
                                .mapConcat(identity(_))
                          }

            _        <- consumer1.merge(ZStream.unwrap(ZIO.sleep(10.seconds).as(consumer2))).runCollect

          } yield assertCompletes
        }
      }
    ).provideSomeLayer(
      ((Layers.kinesisAsyncClient >>> (Layers.adminClient ++ Layers.client)).orDie ++ zio.test.environment.testEnvironment ++ Clock.live ++ Layers.dynamo.orDie) >>> (ZLayer.identity ++ loggingEnv)
    )

  def withStream[R, A](name: String, shards: Int)(f: ZIO[R, Throwable, A]): ZIO[Has[AdminClient] with R, Throwable, A] =
    (for {
      client <- ZManaged.service[AdminClient]
      _      <- client.createStream(name, shards).toManaged(_ => client.deleteStream(name).orDie)
    } yield ()).use_(f)

  val loggingEnv = Slf4jLogger.make((_, logEntry) => logEntry, Some("NativeConsumerTest"))

  def produceSampleRecords(
    streamName: String,
    nrRecords: Int,
    chunkSize: Int = 100,
    throttle: Option[Duration] = None
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
        .mapChunksM(chunk =>
          producer
            .produceChunk(chunk)
            .tapError(e => putStrLn(s"error: $e").provideLayer(Console.live))
            .retry(retryOnResourceNotFound)
            .fork
            .map(fib => Chunk.single(fib))
            .tap(_ => throttle.map(ZIO.sleep(_)).getOrElse(UIO.unit))
        )
        .mapMPar(24)(_.join)
        .runDrain
    }

}
