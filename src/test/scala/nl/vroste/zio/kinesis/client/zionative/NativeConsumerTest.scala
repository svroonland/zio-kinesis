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
import zio.logging.slf4j.Slf4jLogger
import zio.Ref
import zio.Promise
import nl.vroste.zio.kinesis.client.zionative.DiagnosticEvent.PollComplete
import nl.vroste.zio.kinesis.client.zionative.DiagnosticEvent.ShardLeaseLost
import nl.vroste.zio.kinesis.client.zionative.dynamodb.LeaseTable
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import nl.vroste.zio.kinesis.client.Producer.ProduceResponse
import software.amazon.awssdk.services.kinesis.model.Shard
import nl.vroste.zio.kinesis.client.zionative.dynamodb.LeaseCoordinationSettings
import java.time.Instant
import nl.vroste.zio.kinesis.client.zionative.DiagnosticEvent.Checkpoint
import nl.vroste.zio.kinesis.client.zionative.DiagnosticEvent.LeaseReleased
import nl.vroste.zio.kinesis.client.zionative.DiagnosticEvent.LeaseAcquired

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
  - [X] Two workers must be able to start up concurrently
  - [X] When one of a group of workers fail, the others must take over its leases
  - [X] When one of two workers stops gracefully, the other must take over the shards
  - [X] Restart a shard stream when the user has ended it
  - [ ] Recover from a lost connection
   */

  def streamPrefix = ju.UUID.randomUUID().toString().take(6)

  override def spec =
    suite("ZIO Native Kinesis Stream Consumer")(
      testM("retrieve records from all shards") {
        val nrRecords = 2000
        val nrShards  = 5

        withRandomStreamAndApplicationName(nrShards) {
          (streamName, applicationName) =>
            for {
              producer <- produceSampleRecords(streamName, nrRecords, chunkSize = 500).fork
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
                           .take(nrRecords.toLong)
                           .runCollect
              shardIds <- ZIO.service[AdminClient].flatMap(_.describeStream(streamName)).map(_.shards.map(_.shardId()))
              _        <- producer.interrupt

            } yield assert(records.map(_.shardId).toSet)(equalTo(shardIds.toSet))
        }
      },
      testM("release leases at shutdown") {
        val nrRecords = 200
        val nrShards  = 5

        withRandomStreamAndApplicationName(nrShards) {
          (streamName, applicationName) =>
            for {
              _      <- produceSampleRecords(streamName, nrRecords)
              _      <- Consumer
                     .shardedStream(
                       streamName,
                       applicationName,
                       Serde.asciiString,
                       emitDiagnostic = onDiagnostic("worker1")
                     )
                     .flatMapPar(Int.MaxValue) {
                       case (shard @ _, shardStream, checkpointer) =>
                         shardStream.map((_, checkpointer))
                     }
                     .tap { case (r, checkpointer) => checkpointer.stage(r) }
                     .map(_._1)
                     .take(nrRecords.toLong)
                     .runDrain
              result <- assertAllLeasesReleased(applicationName)
            } yield result
        }
      },
      testM("checkpoint the last staged record at shutdown") {
        val nrRecords = 200
        val nrShards  = 5

        withRandomStreamAndApplicationName(nrShards) {
          (streamName, applicationName) =>
            for {
              producedShardsAndSequence <-
                produceSampleRecords(streamName, nrRecords, chunkSize = 500) // Deterministic order
              records            <- Consumer
                           .shardedStream(
                             streamName,
                             applicationName,
                             Serde.asciiString,
                             emitDiagnostic = onDiagnostic("worker1")
                           )
                           .flatMapPar(Int.MaxValue) {
                             case (shard @ _, shardStream, checkpointer) => shardStream.tap(checkpointer.stage)
                           }
                           .take(nrRecords.toLong)
                           .runCollect
              lastSeqNrByShard    = records.groupBy(_.shardId).view.mapValues(_.last)
              checkpoints        <- getCheckpoints(applicationName)
              expectedCheckpoints =
                producedShardsAndSequence.groupBy(_.shardId).view.mapValues(_.last.sequenceNumber).toMap

            } yield assert(checkpoints)(Assertion.hasSameElements(expectedCheckpoints))
        }
      },
      testM("continue from the next message after the last checkpoint") {
        val nrRecords = 200
        val nrShards  = 1

        withRandomStreamAndApplicationName(nrShards) {
          (streamName, applicationName) =>
            for {
              producer <- produceSampleRecords(streamName, nrRecords, chunkSize = nrRecords).fork

              _ <- Consumer
                     .shardedStream(
                       streamName,
                       applicationName,
                       Serde.asciiString,
                       emitDiagnostic = onDiagnostic("Worker1")
                     )
                     .flatMapPar(Int.MaxValue) {
                       case (shard @ _, shardStream, checkpointer) =>
                         shardStream.map((_, checkpointer))
                     }
                     .take(nrRecords.toLong)
                     .tap { case (r, checkpointer) => checkpointer.stage(r) }
                     .runDrain
              _ <- producer.join

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
        val nrRecords = 2000
        val nrShards  = 5

        withRandomStreamAndApplicationName(nrShards) {
          (streamName, applicationName) =>
            for {
              producer                   <- produceSampleRecords(streamName, nrRecords, chunkSize = 10, throttle = Some(1.second)).fork
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
              _                          <- producer.interrupt

            } yield assert(shardsConsumer2)(contains(completedShard))
        }
      },
      testM("workers should be able to start concurrently and both get some shards") {
        val nrRecords = 2000
        val nrShards  = 5

        withRandomStreamAndApplicationName(nrShards) {
          (streamName, applicationName) =>
            for {
              producer                   <- produceSampleRecords(streamName, nrRecords, chunkSize = 50).fork
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
      },
      testM("workers must take over from a stopped consumer") {
        val nrRecords = 2000
        val nrShards  = 7

        withRandomStreamAndApplicationName(nrShards) {
          (streamName, applicationName) =>
            def consumer(workerId: String, emitDiagnostic: DiagnosticEvent => UIO[Unit]) =
              Consumer
                .shardedStream(
                  streamName,
                  applicationName,
                  Serde.asciiString,
                  workerId = workerId,
                  emitDiagnostic = emitDiagnostic,
                  leaseCoordinationSettings = LeaseCoordinationSettings(10.seconds, 3.seconds)
                )
                .flatMapPar(Int.MaxValue) {
                  case (shard @ _, shardStream, checkpointer) =>
                    shardStream
                      .tap(checkpointer.stage)
                      .aggregateAsyncWithin(ZTransducer.collectAllN(200), Schedule.fixed(1.second))
                      .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
                      .map(_.last)
                      .tap(_ => checkpointer.checkpoint)
                      .catchAll {
                        case Right(_) =>
                          ZStream.empty
                        case Left(e)  => ZStream.fail(e)
                      }
                }

            for {
              producer                   <- produceSampleRecords(streamName, nrRecords, chunkSize = 50).fork
              shardsProcessedByConsumer2 <- Ref.make[Set[String]](Set.empty)
              events                     <- Ref.make[List[(String, Instant, DiagnosticEvent)]](List.empty)
              emitDiagnostic              = (workerId: String) =>
                                 (event: DiagnosticEvent) =>
                                   //  onDiagnostic(workerId)(event) *>
                                   zio.clock.currentDateTime
                                     .map(_.toInstant())
                                     .orDie
                                     .flatMap(time => events.update(_ :+ (workerId, time, event)))
                                     .provideLayer(Clock.live)

              consumer1Done              <- Promise.make[Nothing, Unit]
              stream                     <- ZStream
                          .mergeAll(3)(
                            consumer("worker1", emitDiagnostic("worker1"))
                              .take(10) // Such that it has had time to claim some leases
                              .ensuringFirst(log.warn("worker1 done") *> consumer1Done.succeed(())),
                            consumer("worker2", emitDiagnostic("worker2")).ensuringFirst(log.warn("worker2 DONE")),
                            consumer("worker3", emitDiagnostic("worker3")).ensuringFirst(log.warn("Worker3 DONE"))
                          )
                          .runDrain
                          .fork
              _                          <- consumer1Done.await *> ZIO.sleep(10.seconds)
              _                          <- putStrLn("Interrupting producer and stream")
              _                          <- producer.interrupt
              _                          <- stream.interrupt
              allEvents                  <-
                events.get.map(_.filterNot(_._3.isInstanceOf[PollComplete]).filterNot(_._3.isInstanceOf[Checkpoint]))
              // _                           = println(allEvents.mkString("\n"))

              // Workers 2 and 3 should have later-timestamped LeaseAcquired for all shards that were released by Worker 1
              worker1Released      = allEvents.collect { case ("worker1", time, LeaseReleased(shard)) => time -> shard }
              releaseTime          = worker1Released.last._1
              acquiredAfterRelease = allEvents.collect {
                                       case (worker, time, LeaseAcquired(shard, _))
                                           if worker != "worker1" && !time.isBefore(releaseTime) =>
                                         shard
                                     }

            } yield assert(acquiredAfterRelease)(hasSameElements(worker1Released.map(_._2)))
        }
      },
      testM("workers must take over from a zombie consumer") {
        val nrRecords = 2000
        val nrShards  = 7

        withRandomStreamAndApplicationName(nrShards) {
          (streamName, applicationName) =>
            def consumer(
              workerId: String,
              emitDiagnostic: DiagnosticEvent => UIO[Unit],
              checkpointInterval: Duration = 1.second,
              expirationTime: Duration = 10.seconds,
              renewInterval: Duration = 3.seconds
            ) =
              Consumer
                .shardedStream(
                  streamName,
                  applicationName,
                  Serde.asciiString,
                  workerId = workerId,
                  emitDiagnostic = emitDiagnostic,
                  leaseCoordinationSettings = LeaseCoordinationSettings(expirationTime, renewInterval)
                )
                .flatMapPar(Int.MaxValue) {
                  case (shard @ _, shardStream, checkpointer) =>
                    shardStream
                      .tap(checkpointer.stage)
                      .aggregateAsyncWithin(ZTransducer.collectAllN(200000), Schedule.fixed(checkpointInterval))
                      .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
                      .map(_.last)
                      .tap(_ => checkpointer.checkpoint)
                      .catchAll {
                        case Right(_) =>
                          ZStream.empty
                        case Left(e)  => ZStream.fail(e)
                      }
                }

            // The events we have to wait for: 1. At least one LeaseAcquired by worker 1. 2. LeaseAcquired for the same shard by another worker, but it's not a steal (?)
            // maybe NOT a LeaseReleased by worker 1
            def testIsComplete(events: List[(String, Instant, DiagnosticEvent)]) = {
              for {
                acquiredByWorker1      <- Some(events.collect {
                                       case (worker, time, event: LeaseAcquired) if worker == "worker1" => event.shardId
                                     }).filter(_.nonEmpty)
                acquiredByOtherWorkers <- Some(events.collect {
                                            case (worker, time, event: LeaseAcquired) if worker != "worker1" =>
                                              event.shardId
                                          }).filter(_.nonEmpty)
              } yield acquiredByWorker1.toSet subsetOf acquiredByOtherWorkers.toSet
            }.getOrElse(false)

            for {
              producer   <- produceSampleRecords(streamName, nrRecords, chunkSize = 50).fork
              done       <- Promise.make[Nothing, Unit]
              events     <- Ref.make[List[(String, Instant, DiagnosticEvent)]](List.empty)
              handleEvent = (workerId: String) =>
                              (event: DiagnosticEvent) =>
                                onDiagnostic(workerId)(event) *>
                                  zio.clock.currentDateTime
                                    .map(_.toInstant())
                                    .orDie
                                    .flatMap(time => events.update(_ :+ (workerId, time, event)))
                                    .provideLayer(Clock.live) *> events.get.flatMap(events =>
                                  done.succeed(()).when(testIsComplete(events))
                                )

              stream     <- ZStream
                          .mergeAll(3)(
                            // The zombie consumer: not updating checkpoints and not renewing leases
                            consumer(
                              "worker1",
                              handleEvent("worker1"),
                              checkpointInterval = 5.minutes,
                              renewInterval = 5.minutes,
                              expirationTime = 10.minutes
                            ),
                            consumer("worker2", handleEvent("worker2")).ensuringFirst(log.warn("worker2 DONE")),
                            consumer("worker3", handleEvent("worker3")).ensuringFirst(log.warn("Worker3 DONE"))
                          )
                          .runDrain
                          .fork
              _          <- done.await
              _          <- putStrLn("Interrupting producer and stream")
              _          <- producer.interrupt
              _          <- stream.interrupt
              allEvents  <-
                events.get.map(_.filterNot(_._3.isInstanceOf[PollComplete]).filterNot(_._3.isInstanceOf[Checkpoint]))
              _           = println(allEvents.mkString("\n"))
            } yield assertCompletes
        }
      },
      testM("a worker must pick up an ended shard stream") {
        val nrRecords = 2000
        val nrShards  = 3

        withRandomStreamAndApplicationName(nrShards) {
          (streamName, applicationName) =>
            def consumer(workerId: String, emitDiagnostic: DiagnosticEvent => UIO[Unit]) =
              Consumer
                .shardedStream(
                  streamName,
                  applicationName,
                  Serde.asciiString,
                  workerId = workerId,
                  emitDiagnostic = emitDiagnostic,
                  leaseCoordinationSettings = LeaseCoordinationSettings(10.seconds, 3.seconds)
                )
                .flatMapPar(Int.MaxValue) {
                  case (shard @ _, shardStream, checkpointer) =>
                    val out = shardStream
                      .tap(checkpointer.stage)
                      .aggregateAsyncWithin(ZTransducer.collectAllN(200), Schedule.fixed(1.second))
                      .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
                      .map(_.last)
                      .tap(_ => checkpointer.checkpoint)
                      .catchAll {
                        case Right(_) =>
                          ZStream.empty
                        case Left(e)  => ZStream.fail(e)
                      }

                    if (shard == "shardId-000000000001") out.take(3) else out
                }

            // TODO extract DiagnosticEventList { def eventsByWorker(workerId), eventsAfter(instant), etc }
            for {
              producer      <- produceSampleRecords(streamName, nrRecords, chunkSize = 50).fork
              done          <- Promise.make[Nothing, Unit]
              events        <- Ref.make[List[DiagnosticEvent]](List.empty)
              emitDiagnostic =
                (workerId: String) =>
                  (event: DiagnosticEvent) =>
                    onDiagnostic(workerId)(event) *>
                      events.update(_ :+ event) *>
                      done
                        .succeed(())
                        .whenM(events.get.map(_.collect { case _: LeaseAcquired => 1 }.sum == nrShards + 1))

              consumer1Done <- Promise.make[Nothing, Unit]
              stream        <- consumer("worker1", emitDiagnostic("worker1")).runDrain.fork
              _             <- done.await
              _             <- producer.interrupt
              _             <- stream.interrupt
            } yield assertCompletes
        }
      }
    ).provideSomeLayer(env) @@
      TestAspect.timed @@
      TestAspect.timeoutWarning(30.seconds) @@
      TestAspect.timeout(120.seconds)

  val env =
    ((Layers.kinesisAsyncClient >>>
      (Layers.adminClient ++ Layers.client)).orDie ++
      zio.test.environment.testEnvironment ++
      Clock.live ++
      Layers.dynamo.orDie) >>>
      (ZLayer.identity ++ loggingEnv)

  def withStream[R, A](name: String, shards: Int)(
    f: ZIO[R, Throwable, A]
  ): ZIO[Has[AdminClient] with Has[Client] with Clock with R, Throwable, A] =
    (for {
      client <- ZManaged.service[AdminClient]
      _      <- client.createStream(name, shards).toManaged(_ => client.deleteStream(name).orDie)
      // Wait for the stream to have shards
      _      <- {
        def getShards: ZIO[Has[Client] with Clock, Throwable, Chunk[Shard]] =
          ZIO.service[Client].flatMap(_.listShards(name).runCollect).filterOrElse(_.nonEmpty)(_ => getShards)
        getShards.toManaged_
      }
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

  def assertAllLeasesReleased(applicationName: String) =
    for {
      table  <- ZIO.service[DynamoDbAsyncClient].map(new LeaseTable(_, applicationName))
      leases <- table.getLeasesFromDB
    } yield assert(leases)(forall(hasField("owner", _.owner, isNone)))

  def getCheckpoints(applicationName: String) =
    for {
      table      <- ZIO.service[DynamoDbAsyncClient].map(new LeaseTable(_, applicationName))
      leases     <- table.getLeasesFromDB
      checkpoints = leases.collect {
                      case l if l.checkpoint.isDefined => l.key -> l.checkpoint.get.sequenceNumber
                    }.toMap
    } yield checkpoints

  def withRandomStreamAndApplicationName[R, A](
    nrShards: Int
  )(
    f: (String, String) => ZIO[R, Throwable, A]
  ): ZIO[Has[Client] with Has[AdminClient] with Clock with R, Throwable, A] =
    ZIO.effectTotal((streamPrefix + "testStream", streamPrefix + "testApplication")).flatMap {
      case (streamName, applicationName) =>
        withStream(streamName, shards = nrShards) {
          f(streamName, applicationName)
        }
    }

}
