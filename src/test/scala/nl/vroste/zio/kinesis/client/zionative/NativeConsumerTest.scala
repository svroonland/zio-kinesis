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

import zio.logging.log
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
// import zio.logging.log
import zio.logging.slf4j.Slf4jLogger
import zio.Ref
import zio.Promise
import nl.vroste.zio.kinesis.client.zionative.DiagnosticEvent.PollComplete
import nl.vroste.zio.kinesis.client.zionative.DiagnosticEvent.ShardLeaseLost
import nl.vroste.zio.kinesis.client.zionative.dynamodb.LeaseTable
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import nl.vroste.zio.kinesis.client.Producer.ProduceResponse
// import zio.logging.LogAnnotation

object NativeConsumerTest extends DefaultRunnableSpec {
  /*
  - [X] It must retrieve records from all shards
  - [ ] Support both polling and enhanced fanout
  - [X] Must restart from the given initial start point if no lease yet
  - [X] Must restart from the record after the last checkpointed record for each shard
  - [X] Should release leases at shutdown (another worker should aquicre all leases directly without having to steal)
  - [X] Must checkpoint the last staged checkpoint before shutdown
  - [ ] Correctly deserialize the records
  - [ ] something about rate limits: maybe if we have multiple consumers active and run into rate limits?
  - [X] Steal an equal share of leases from another worker
  - [ ] Two workers must be able to start up concurrently (not sure what the assertion would be)
  - [ ] When one of two workers fail, the other must take over (TODO how to simulate crashing worker)
  - [ ] When one of two workers stops gracefully, the other must take over the shards
  - [ ] Restart a shard stream when the user has ended it
  - [ ] Recover from a lost connection
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
            _        <- log.info("Producing records")
            _        <- produceSampleRecords(streamName, nrRecords, chunkSize = 500).fork
            _        <- log.info("Starting consumer 1")
            records  <- Consumer
                         .shardedStream(
                           streamName,
                           s"${streamName}-test1",
                           Serde.asciiString,
                           fetchMode = FetchMode.Polling(batchSize = 1000),
                           emitDiagnostic = onDiagnostic("worker1")
                         )
                         .flatMapPar(Int.MaxValue) {
                           case (shard @ _, shardStream, checkpointer) =>
                             shardStream.tap(checkpointer.stage)
                         }
                         //  .tap(r => UIO(println(s"Got record on shard ${r.shardId}")))
                         .take(nrRecords.toLong)
                         .runCollect
            shardIds <- ZIO.service[AdminClient].flatMap(_.describeStream(streamName)).map(_.shards.map(_.shardId()))

          } yield assert(records.map(_.shardId).toSet)(equalTo(shardIds.toSet))
        }
      },
      testM("release leases at shutdown") {
        val streamName      = streamPrefix + "testStream-2"
        val applicationName = streamPrefix + "test2"
        val nrRecords       = 200
        val nrShards        = 5

        withStream(streamName, shards = nrShards) {
          for {
            _      <- produceSampleRecords(streamName, nrRecords)
            _      <-
              Consumer
                .shardedStream(streamName, applicationName, Serde.asciiString, emitDiagnostic = onDiagnostic("worker1"))
                .flatMapPar(Int.MaxValue) {
                  case (shard @ _, shardStream, checkpointer) =>
                    shardStream.map((_, checkpointer))
                }
                .tap { case (r, checkpointer) => checkpointer.stage(r) }
                .map(_._1)
                .take(nrRecords.toLong)
                .runDrain

            table  <- ZIO.service[DynamoDbAsyncClient].map(new LeaseTable(_, applicationName))
            leases <- table.getLeasesFromDB
          } yield assert(leases)(forall(hasField("owner", _.owner, isNone)))
        }
      },
      testM("checkpoint the last staged record at shutdown") {
        val streamName      = streamPrefix + "testStream-2"
        val applicationName = streamPrefix + "test2"
        val nrRecords       = 200
        val nrShards        = 5

        withStream(streamName, shards = nrShards) {
          for {
            producedShardsAndSequence <-
              produceSampleRecords(streamName, nrRecords, chunkSize = 500) // Deterministic order
            records            <-
              Consumer
                .shardedStream(streamName, applicationName, Serde.asciiString, emitDiagnostic = onDiagnostic("worker1"))
                .flatMapPar(Int.MaxValue) {
                  case (shard @ _, shardStream, checkpointer) =>
                    shardStream
                      .tap(checkpointer.stage)
                }
                .take(nrRecords.toLong)
                .runCollect
            lastSeqNrByShard    = records.groupBy(_.shardId).view.mapValues(_.last) // maxBy(_.sequenceNumber.toLong))
            _                  <- ZIO.sleep(1.second)                               // DynamoDB eventual consistency..
            table              <- ZIO.service[DynamoDbAsyncClient].map(new LeaseTable(_, applicationName))
            leases             <- table.getLeasesFromDB
            checkpoints         = leases.collect {
                            case l if l.checkpoint.isDefined => l.key -> l.checkpoint.get.sequenceNumber
                          }.toMap
            expectedCheckpoints =
              producedShardsAndSequence.groupBy(_.shardId).view.mapValues(_.last.sequenceNumber).toMap

          } yield assert(checkpoints)(Assertion.hasSameElements(expectedCheckpoints))
        }
      },
      testM("continue from the next message after the last checkpoint") {
        val streamName      = streamPrefix + "testStream-2"
        val applicationName = streamPrefix + "test2"
        val nrRecords       = 200
        val nrShards        = 1

        withStream(streamName, shards = nrShards) {
          for {
            _ <- produceSampleRecords(streamName, nrRecords, chunkSize = nrRecords).fork
            _ <-
              Consumer
                .shardedStream(streamName, applicationName, Serde.asciiString, emitDiagnostic = onDiagnostic("Worker1"))
                .flatMapPar(Int.MaxValue) {
                  case (shard @ _, shardStream, checkpointer) =>
                    shardStream.map((_, checkpointer))
                }
                .take(nrRecords.toLong)
                .tap {
                  case (r, checkpointer) => checkpointer.stage(r)
                } // It will automatically checkpoint at stream end
                .runDrain

            _ <- produceSampleRecords(streamName, 1, indexStart = nrRecords + 3) // Something arbitrary

            // TODO improveee test: this could also pass with shard iterator LATEST or something
            firstRecord <- Consumer
                             .shardedStream(
                               streamName,
                               applicationName,
                               Serde.asciiString,
                               workerId = "worker2",
                               emitDiagnostic = onDiagnostic("worker2")
                             )
                             .flatMapPar(Int.MaxValue) {
                               case (shard @ _, shardStream, checkpointer) => shardStream.map((_, checkpointer))
                             }
                             .tap {
                               case (r, checkpointer) => checkpointer.stage(r)
                             } // It will automatically checkpoint at stream end
                             .map(_._1)
                             .take(1)
                             .runHead

          } yield assert(firstRecord)(isSome(hasField("key", _.partitionKey, equalTo(s"key${nrRecords + 3}"))))
        }
      },
      testM("worker steals leases from other worker until they both have an equal share") {
        val streamName      = streamPrefix + "testStream-3"
        val applicationName = streamPrefix + "test3"
        val nrRecords       = 20000
        val nrShards        = 5

        withStream(streamName, shards = nrShards) {

          for {
            _                          <- produceSampleRecords(streamName, nrRecords, chunkSize = 10, throttle = Some(1.second)).fork
            shardsProcessedByConsumer2 <- Ref.make[Set[String]](Set.empty)
            shardCompletedByConsumer1  <- Promise.make[Nothing, String]
            shardStartedByConsumer2    <- Promise.make[Nothing, Unit]
            consumer1                   = Consumer
                          .shardedStream(
                            streamName,
                            applicationName,
                            Serde.asciiString,
                            workerId = "worker1",
                            emitDiagnostic = onDiagnostic("worker1")
                          )
                          .flatMapPar(Int.MaxValue) {
                            case (shard, shardStream, checkpointer) =>
                              shardStream
                              // .tap(r => UIO(println(s"Worker 1 got record on shard ${r.shardId}")))
                                .tap(checkpointer.stage)
                                .aggregateAsyncWithin(ZTransducer.collectAllN(20), Schedule.fixed(1.second))
                                .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
                                .tap(_ => checkpointer.checkpoint)
                                .catchAll {
                                  case Right(e) =>
                                    println(s"Got error ${e}")
                                    ZStream.empty
                                  case Left(e)  =>
                                    println(s"Got error left ${e}")
                                    ZStream.fail(e)
                                }
                                .mapConcat(identity(_))
                                .ensuring(shardCompletedByConsumer1.succeed(shard))
                          }
            consumer2                   = Consumer
                          .shardedStream(
                            streamName,
                            applicationName,
                            Serde.asciiString,
                            workerId = "worker2",
                            emitDiagnostic = onDiagnostic("worker2")
                          )
                          .flatMapPar(Int.MaxValue) {
                            case (shard, shardStream, checkpointer) =>
                              ZStream.fromEffect(
                                shardStartedByConsumer2.succeed(()) *> shardsProcessedByConsumer2.update(_ + shard)
                              ) *>
                                shardStream
                                // .tap(r => UIO(println(s"Worker 2 got record on shard ${r.shardId}")))
                                  .tap(checkpointer.stage)
                                  .aggregateAsyncWithin(ZTransducer.collectAllN(20), Schedule.fixed(1.second))
                                  .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
                                  .tap(_ => checkpointer.checkpoint)
                                  .catchAll {
                                    case Right(_) =>
                                      ZStream.empty
                                    case Left(e)  => ZStream.fail(e)
                                  }
                                  .mapConcat(identity(_))
                          }

            fib                        <- consumer1.merge(ZStream.unwrap(ZIO.sleep(8.seconds).as(consumer2))).runCollect.fork
            completedShard             <- shardCompletedByConsumer1.await
            _                          <- shardStartedByConsumer2.await
            shardsConsumer2            <- shardsProcessedByConsumer2.get
            _                          <- fib.interrupt

          } yield assert(shardsConsumer2)(contains(completedShard))
        }
      },
      testM("workers should be able to start concurrently and both get some shards") {
        val streamName      = streamPrefix + "testStream-3"
        val applicationName = streamPrefix + "test3"
        val nrRecords       = 20000
        val nrShards        = 5

        withStream(streamName, shards = nrShards) {

          for {
            producer                   <- produceSampleRecords(streamName, nrRecords, chunkSize = 500).fork
            shardsProcessedByConsumer2 <- Ref.make[Set[String]](Set.empty)

            // Spin up two workers, let them fight a bit over leases
            consumer1 = Consumer
                          .shardedStream(
                            streamName,
                            applicationName,
                            Serde.asciiString,
                            workerId = "worker1",
                            emitDiagnostic = onDiagnostic("worker1")
                          )
                          .flatMapPar(Int.MaxValue) {
                            case (shard @ _, shardStream, checkpointer) =>
                              shardStream
                                .tap(checkpointer.stage)
                                .aggregateAsyncWithin(ZTransducer.collectAllN(200), Schedule.fixed(1.second))
                                .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
                                .tap(_ => checkpointer.checkpoint)
                                .catchAll {
                                  case Right(_) =>
                                    ZStream.empty
                                  case Left(e)  => ZStream.fail(e)
                                }
                                .mapConcat(identity(_))
                          }
                          .take(10)
            consumer2 = Consumer
                          .shardedStream(
                            streamName,
                            applicationName,
                            Serde.asciiString,
                            workerId = "worker2",
                            emitDiagnostic = onDiagnostic("worker2")
                          )
                          .flatMapPar(Int.MaxValue) {
                            case (shard, shardStream, checkpointer) =>
                              ZStream.fromEffect(shardsProcessedByConsumer2.update(_ + shard)) *>
                                shardStream
                                  .tap(checkpointer.stage)
                                  .aggregateAsyncWithin(ZTransducer.collectAllN(200), Schedule.fixed(1.second))
                                  .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
                                  .tap(_ => checkpointer.checkpoint)
                                  .catchAll {
                                    case Right(_) =>
                                      ZStream.empty
                                    case Left(e)  => ZStream.fail(e)
                                  }
                                  .mapConcat(identity(_))
                                  .ensuring(UIO(println(s"Shard stream worker 2 ${shard} completed")))
                          }
                          .take(10)

            _        <- consumer1.merge(consumer2).runCollect
            _        <- producer.interrupt
          } yield assertCompletes
        }
      }
    ).provideSomeLayer(env) @@ TestAspect.timed @@ TestAspect.sequential

  val env =
    ((Layers.kinesisAsyncClient >>>
      (Layers.adminClient ++ Layers.client)).orDie ++
      zio.test.environment.testEnvironment ++
      Clock.live ++
      Layers.dynamo.orDie) >>>
      (ZLayer.identity ++ loggingEnv)

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
    throttle: Option[Duration] = None,
    indexStart: Int = 1
  ): ZIO[Has[Client] with Clock, Throwable, Chunk[ProduceResponse]] =
    (for {
      client   <- ZIO.service[Client].toManaged_
      producer <- Producer.make(streamName, client, Serde.asciiString)
    } yield producer).use { producer =>
      val records =
        (indexStart until (nrRecords + indexStart)).map(i => ProducerRecord(s"key$i", s"msg$i"))
      ZStream
        .fromIterable(records)
        .chunkN(chunkSize)
        .mapChunksM { chunk =>
          producer
            .produceChunk(chunk)
            .tapError(e => putStrLn(s"error: $e").provideLayer(Console.live))
            .retry(retryOnResourceNotFound)
            .tap(_ => throttle.map(ZIO.sleep(_)).getOrElse(UIO.unit))
            .map(Chunk.fromIterable)
        }
        .runCollect
        .map(Chunk.fromIterable)
    }

  def produceSampleRecordsMassivelyParallel(
    streamName: String,
    nrRecords: Int,
    chunkSize: Int = 100,
    indexStart: Int = 1
  ): ZIO[Has[Client] with Clock, Throwable, Chunk[ProduceResponse]] =
    (for {
      client   <- ZIO.service[Client].toManaged_
      producer <- Producer.make(streamName, client, Serde.asciiString)
    } yield producer).use { producer =>
      val records =
        (indexStart until (nrRecords + indexStart)).map(i => ProducerRecord(s"key$i", s"msg$i"))
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
        )
        .mapMPar(24)(_.join)
        .mapConcatChunk(Chunk.fromIterable)
        .runCollect
        .map(Chunk.fromIterable)
    }

  def onDiagnostic(worker: String) =
    (ev: DiagnosticEvent) =>
      ev match {
        case _: PollComplete => UIO.unit
        case _               => log.info(s"${worker}: ${ev}").provideLayer(loggingEnv)
      }

}
