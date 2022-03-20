package nl.vroste.zio.kinesis.client.zionative

import io.github.vigoo.zioaws.cloudwatch.CloudWatch
import io.github.vigoo.zioaws.dynamodb.DynamoDb
import io.github.vigoo.zioaws.kinesis
import io.github.vigoo.zioaws.kinesis.Kinesis
import io.github.vigoo.zioaws.kinesis.model.{ DescribeStreamRequest, ScalingType, UpdateShardCountRequest }
import nl.vroste.zio.kinesis.client
import nl.vroste.zio.kinesis.client.Producer.ProduceResponse
import nl.vroste.zio.kinesis.client.TestUtil.{ retryOnResourceNotFound, withStream }
import nl.vroste.zio.kinesis.client._
import nl.vroste.zio.kinesis.client.localstack.LocalStackServices
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.zionative.DiagnosticEvent.PollComplete
import nl.vroste.zio.kinesis.client.zionative.leasecoordinator.LeaseCoordinationSettings
import nl.vroste.zio.kinesis.client.zionative.leaserepository.DynamoDbLeaseRepository
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console._
import zio.duration._
import zio.logging._
import zio.stream.{ ZStream, ZTransducer }
import zio.test.Assertion._
import zio.test._

import java.time.Instant
import java.{ util => ju }

object NativeConsumerTest extends DefaultRunnableSpec {
  /*
  - [X] It must retrieve records from all shards
  - [X] Support both polling and enhanced fanout
  - [X] Must restart from the given initial start point if no lease yet
  - [X] Must restart from the record after the last checkpointed record for each shard
  - [X] Should release leases at shutdown (another worker should acquire all leases directly without having to steal)
  - [X] Must checkpoint the last staged checkpoint before shutdown
  - [ ] Correctly deserialize the records
  - [ ] something about rate limits: maybe if we have multiple consumers active and run into rate limits?
  - [X] Steal an equal share of leases from another worker
  - [X] Two workers must be able to start up concurrently
  - [X] When one of a group of workers fail, the others must take over its leases
  - [X] When one of two workers stops gracefully, the other must take over the shards
  - [X] Restart a shard stream when the user has ended it
  - [ ] Release leases after they have expired during a connection failure
  - [ ] Recover from a lost connection
   */

  def streamPrefix = ju.UUID.randomUUID().toString.take(9)

  override def spec =
    suite("ZIO Native Kinesis Stream Consumer")(
      testM("retrieve records from all shards") {
        val nrRecords = 2000
        val nrShards  = 5

        withRandomStreamAndApplicationName(nrShards) { (streamName, applicationName) =>
          for {
            _        <- log.info("Starting producer")
            producer <- produceSampleRecords(streamName, nrRecords, chunkSize = 500).fork
            _        <- log.info("Starting consumer")
            records  <- Consumer
                          .shardedStream(
                            streamName,
                            applicationName,
                            Serde.asciiString,
                            fetchMode = FetchMode.Polling(batchSize = 1000),
                            emitDiagnostic = onDiagnostic("worker1")
                          )
                          .flatMapPar(Int.MaxValue) { case (shard @ _, shardStream, checkpointer) =>
                            shardStream.tap(checkpointer.stage)
                          }
                          .take(nrRecords.toLong)
                          .runCollect
            shardIds <- kinesis
                          .describeStream(DescribeStreamRequest(streamName))
                          .mapError(_.toThrowable)
                          .map(_.streamDescriptionValue.shardsValue.map(_.shardIdValue))
            _        <- producer.interrupt

          } yield assert(records.map(_.shardId).toSet)(equalTo(shardIds.toSet))
        }
      },
      testM("release leases at shutdown") {
        val nrRecords = 200
        val nrShards  = 5

        withRandomStreamAndApplicationName(nrShards) { (streamName, applicationName) =>
          for {
            _      <- produceSampleRecords(streamName, nrRecords)
            _      <- Consumer
                        .shardedStream(
                          streamName,
                          applicationName,
                          Serde.asciiString,
                          emitDiagnostic = onDiagnostic("worker1")
                        )
                        .flatMapPar(Int.MaxValue) { case (shard @ _, shardStream, checkpointer) =>
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

        withRandomStreamAndApplicationName(nrShards) { (streamName, applicationName) =>
          for {
            producedShardsAndSequence <-
              produceSampleRecords(streamName, nrRecords, chunkSize = 500) // Deterministic order
            _                         <- Consumer
                                           .shardedStream(
                                             streamName,
                                             applicationName,
                                             Serde.asciiString,
                                             emitDiagnostic = onDiagnostic("worker1")
                                           )
                                           .flatMapPar(Int.MaxValue) { case (shard @ _, shardStream, checkpointer) =>
                                             shardStream.tap(checkpointer.stage)
                                           }
                                           .take(nrRecords.toLong)
                                           .runCollect
            checkpoints               <- getCheckpoints(applicationName)
            expectedCheckpoints        =
              producedShardsAndSequence.groupBy(_.shardId).view.mapValues(_.last.sequenceNumber).toMap

          } yield assert(checkpoints)(Assertion.hasSameElements(expectedCheckpoints))
        }
      },
      testM("continue from the next message after the last checkpoint") {
        val nrRecords = 200
        val nrShards  = 1

        withRandomStreamAndApplicationName(nrShards) { (streamName, applicationName) =>
          for {
            producer <- produceSampleRecords(streamName, nrRecords, chunkSize = nrRecords).fork

            _ <- Consumer
                   .shardedStream(
                     streamName,
                     applicationName,
                     Serde.asciiString,
                     emitDiagnostic = onDiagnostic("Worker1")
                   )
                   .flatMapPar(Int.MaxValue) { case (shard @ _, shardStream, checkpointer) =>
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
                               workerIdentifier = "worker2",
                               emitDiagnostic = onDiagnostic("worker2")
                             )
                             .flatMapPar(Int.MaxValue) { case (shard @ _, shardStream, checkpointer) =>
                               shardStream.map((_, checkpointer))
                             }
                             .tap { case (r, checkpointer) =>
                               checkpointer.stage(r)
                             } // It will automatically checkpoint at stream end
                             .map(_._1)
                             .take(1)
                             .runHead

          } yield assert(firstRecord)(
            isSome(hasField("key", (_: Record[String]).partitionKey, equalTo(s"key${nrRecords + 3}")))
          )
        }
      },
      testM("worker steals leases from other worker until they both have an equal share") {
        val nrRecords = 2000
        val nrShards  = 5

        withRandomStreamAndApplicationName(nrShards) { (streamName, applicationName) =>
          for {
            producer         <- produceSampleRecords(streamName, nrRecords, chunkSize = 10, throttle = Some(1.second)).fork
            consumer1Started <- Promise.make[Nothing, Unit]
            consumer1         = Consumer
                                  .shardedStream(
                                    streamName,
                                    applicationName,
                                    Serde.asciiString,
                                    workerIdentifier = "worker1",
                                    emitDiagnostic = onDiagnostic("worker1")
                                  )
                                  .flatMapPar(Int.MaxValue) { case (shard @ _, shardStream, checkpointer) =>
                                    shardStream
                                      // .tap(r => UIO(println(s"Worker 1 got record on shard ${r.shardId}")))
                                      .tap(checkpointer.stage)
                                      .tap(_ => consumer1Started.succeed(()))
                                      .aggregateAsyncWithin(ZTransducer.collectAllN(2000), Schedule.fixed(5.minutes))
                                      .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
                                      .tap(_ => checkpointer.checkpoint())
                                      .catchAll {
                                        case Right(e) =>
                                          println(s"Got error ${e}")
                                          ZStream.empty
                                        case Left(e)  =>
                                          println(s"Got error left ${e}")
                                          ZStream.fail(e)
                                      }
                                      .mapConcat(identity(_))
                                  }
                                  .updateService[Logger[String]](_.named("worker1"))
            consumer2         = Consumer
                                  .shardedStream(
                                    streamName,
                                    applicationName,
                                    Serde.asciiString,
                                    workerIdentifier = "worker2",
                                    emitDiagnostic = onDiagnostic("worker2")
                                  )
                                  .tap(tp => log.info(s"Got tuple ${tp}"))
                                  .take(2) // 5 shards, so we expect 2
                                  .updateService[Logger[String]](_.named("worker2"))
            worker1          <- consumer1.runDrain.tapError(e => log.error(s"Worker1 failed: ${e}")).fork
            _                <- consumer1Started.await
            _                <- log.info("Consumer 1 has started, starting consumer 2")
            _                <- consumer2.runDrain
            _                <- log.info("Shutting down worker 1")
            _                <- worker1.interrupt
            _                <- log.info("Shutting down producer")
            _                <- producer.interrupt

          } yield assertCompletes
        }
      },
      testM("workers should be able to start concurrently and both get some shards") {
        val nrRecords =
          20000 // This should probably be large enough to guarantee that both workers can get enough records to complete
        val nrShards = 5

        withRandomStreamAndApplicationName(nrShards) { (streamName, applicationName) =>
          for {
            producer                   <- produceSampleRecords(streamName, nrRecords, chunkSize = 50, throttle = Some(1.second)).fork
            shardsProcessedByConsumer2 <- Ref.make[Set[String]](Set.empty)

            // Spin up two workers, let them fight a bit over leases
            consumer1 = Consumer
                          .shardedStream(
                            streamName,
                            applicationName,
                            Serde.asciiString,
                            workerIdentifier = "worker1",
                            emitDiagnostic = onDiagnostic("worker1"),
                            leaseCoordinationSettings = LeaseCoordinationSettings(refreshAndTakeInterval = 5.seconds)
                          )
                          .flatMapPar(Int.MaxValue) { case (shard @ _, shardStream, checkpointer) =>
                            shardStream
                              .tap(checkpointer.stage)
                              .aggregateAsyncWithin(ZTransducer.collectAllN(200), Schedule.fixed(1.second))
                              .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
                              // .tap(_ => checkpointer.checkpoint())
                              .catchAll {
                                case Right(_) =>
                                  ZStream.empty
                                case Left(e)  => ZStream.fail(e)
                              }
                              .ensuring(UIO(println(s"Shard stream worker 1 ${shard} completed")))
                          }
                          .tap(_ => log.info("WORKER1 GOT A BATCH"))
                          .take(10)
            consumer2 = Consumer
                          .shardedStream(
                            streamName,
                            applicationName,
                            Serde.asciiString,
                            workerIdentifier = "worker2",
                            emitDiagnostic = onDiagnostic("worker2"),
                            leaseCoordinationSettings = LeaseCoordinationSettings(refreshAndTakeInterval = 5.seconds)
                          )
                          .flatMapPar(Int.MaxValue) { case (shard, shardStream, checkpointer) =>
                            ZStream.fromEffect(shardsProcessedByConsumer2.update(_ + shard)) *>
                              shardStream
                                .tap(checkpointer.stage)
                                .aggregateAsyncWithin(ZTransducer.collectAllN(200), Schedule.fixed(1.second))
                                .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
                                // .tap(_ => checkpointer.checkpoint())
                                .catchAll {
                                  case Right(_) =>
                                    ZStream.empty
                                  case Left(e)  => ZStream.fail(e)
                                }
                                .ensuring(UIO(println(s"Shard stream worker 2 ${shard} completed")))
                          }
                          .tap(_ => log.info("WORKER2 GOT A BATCH"))
                          .take(10)

            _ <- consumer1.merge(consumer2).runCollect
            _ <- producer.interrupt
          } yield assertCompletes
        }
      },
      testM("workers must take over from a stopped consumer") {
        val nrRecords = 200000
        val nrShards  = 7

        def consumer(
          streamName: String,
          applicationName: String,
          workerId: String,
          emitDiagnostic: DiagnosticEvent => UIO[Unit]
        ) =
          Consumer
            .shardedStream(
              streamName,
              applicationName,
              Serde.asciiString,
              workerIdentifier = workerId,
              leaseCoordinationSettings = LeaseCoordinationSettings(
                1.seconds,
                refreshAndTakeInterval = 3.seconds,
                maxParallelLeaseAcquisitions = 1
              ),
              emitDiagnostic = emitDiagnostic
            )
            .flatMapPar(Int.MaxValue) { case (shard @ _, shardStream, checkpointer) =>
              shardStream
                .tap(checkpointer.stage)
                .aggregateAsyncWithin(ZTransducer.collectAllN(200), Schedule.fixed(1.second))
                .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
                .map(_.lastOption)
                .tap(_ => checkpointer.checkpoint())
                .catchAll {
                  case Right(_) =>
                    ZStream.empty
                  case Left(e)  => ZStream.fail(e)
                }
            }
            .catchAll { case e =>
              ZStream.unwrap(log.error(e.toString).as(ZStream.fail(e)))
            }
            .updateService[Logger[String]](_.named(s"worker-${workerId}"))

        withRandomStreamAndApplicationName(nrShards) { (streamName, applicationName) =>
          for {
            consumer1Done <- Promise.make[Throwable, Unit]
            producer      <- produceSampleRecords(streamName, nrRecords, chunkSize = 50, throttle = Some(1.second))
                               .tapError(consumer1Done.fail(_))
                               .fork
            events        <- Ref.make[List[(String, Instant, DiagnosticEvent)]](List.empty)
            emitDiagnostic = (workerId: String) =>
                               (event: DiagnosticEvent) =>
                                 onDiagnostic(workerId)(event) *>
                                   zio.clock.currentDateTime
                                     .map(_.toInstant())
                                     .orDie
                                     .flatMap(time => events.update(_ :+ ((workerId, time, event))))
                                     .provideLayer(Clock.live)

            _         <- {
              for {

                worker1 <- (consumer(streamName, applicationName, "worker1", emitDiagnostic("worker1"))
                             .take(10) // Such that it has had time to claim some leases
                             .runDrain
                             .tapError(e => log.error(s"Worker1 failed with error: ${e}"))
                             .tapError(consumer1Done.fail(_))
                             *> log.warn("worker1 done") *> consumer1Done.succeed(())).fork

                worker2 <- consumer(streamName, applicationName, "worker2", emitDiagnostic("worker2"))
                             .ensuringFirst(
                               log.warn("worker2 DONE")
                             )
                             .runDrain
                             .delay(5.seconds)
                             .fork
                worker3 <- consumer(streamName, applicationName, "worker3", emitDiagnostic("worker3"))
                             .ensuringFirst(
                               log.warn("worker3 DONE")
                             )
                             .runDrain
                             .delay(5.seconds)
                             .fork

                _ <- consumer1Done.await
                _ <- log.debug("Consumer1 is done")
                _ <- ZIO.sleep(10.seconds)
                _ <- log.debug("Interrupting producer")
                _ <- producer.interrupt
                _ <- log.debug("Interrupting streams")
                _ <- worker2.interrupt
                       .tap(_ => log.info("Done interrupting worker 2"))
                       .tapCause(e => log.error("Error interrupting worker 2:", e))
                       .ignore zipPar worker3.interrupt
                       .tap(_ => log.info("Done interrupting worker 3"))
                       .tapCause(e => log.error("Error interrupting worker 3:", e))
                       .ignore zipPar worker1.join
                       .tap(_ => log.info("Done interrupting worker 1"))
                       .tapCause(e => log.error("Error joining worker 1:", e))
                       .ignore
              } yield ()
            }.onInterrupt(
              events.get
                .map(
                  _.filterNot(_._3.isInstanceOf[PollComplete])
                    .filterNot(_._3.isInstanceOf[DiagnosticEvent.Checkpoint])
                )
                .tap(allEvents => UIO(println(allEvents.mkString("\n"))))
            )
            allEvents <- events.get.map(
                           _.filterNot(_._3.isInstanceOf[PollComplete])
                             .filterNot(_._3.isInstanceOf[DiagnosticEvent.Checkpoint])
                         )
            _          = println(allEvents.mkString("\n"))

            // Workers 2 and 3 should have later-timestamped LeaseAcquired for all shards that were released by Worker 1
            worker1Released      = allEvents.collect { case ("worker1", time, DiagnosticEvent.LeaseReleased(shard)) =>
                                     time -> shard
                                   }
            releaseTime          = worker1Released.last._1
            acquiredAfterRelease = allEvents.collect {
                                     case (worker, time, DiagnosticEvent.LeaseAcquired(shard, _))
                                         if worker != "worker1" && !time.isBefore(releaseTime) =>
                                       shard
                                   }

          } yield assert(worker1Released.map(_._2).toSet)(
            hasIntersection(acquiredAfterRelease.toSet)(hasSameElements(worker1Released.map(_._2).toSet))
          )
        }
      },
      testM("workers must take over from a zombie consumer") {
        val nrRecords = 2000
        val nrShards  = 7

        def consumer(
          streamName: String,
          applicationName: String,
          workerId: String,
          emitDiagnostic: DiagnosticEvent => UIO[Unit],
          checkpointInterval: Duration = 1.second,
          renewInterval: Duration = 3.seconds
        ) =
          Consumer
            .shardedStream(
              streamName,
              applicationName,
              Serde.asciiString,
              workerIdentifier = workerId,
              leaseCoordinationSettings = LeaseCoordinationSettings(
                renewInterval = renewInterval,
                refreshAndTakeInterval = checkpointInterval,
                maxParallelLeaseAcquisitions = 1
              ),
              emitDiagnostic = emitDiagnostic
            )
            .flatMapPar(Int.MaxValue) { case (shard @ _, shardStream, checkpointer) =>
              shardStream
                .tap(checkpointer.stage)
                .aggregateAsyncWithin(ZTransducer.collectAllN(200000), Schedule.fixed(checkpointInterval))
                .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
                .map(_.last)
                .tap(_ => checkpointer.checkpoint())
                .catchAll {
                  case Right(_) =>
                    println(s"Worker appears to have lost the lease?")
                    ZStream.empty
                  case Left(e)  =>
                    println(s"Worker ${workerId} failed with ${e}")
                    ZStream.fail(e)
                }
            }

        // The events we have to wait for:
        // 1. At least one LeaseAcquired by worker 1.
        // 2. LeaseAcquired for the same shard by another worker, but it's not a steal (?)
        // maybe NOT a LeaseReleased by worker 1
        def testIsComplete(events: List[(String, Instant, DiagnosticEvent)]) = {
          for {
            acquiredByWorker1      <- Some(events.collect {
                                        case (worker, _, event: DiagnosticEvent.LeaseAcquired) if worker == "worker1" =>
                                          event.shardId
                                      }).filter(_.nonEmpty)
            acquiredByOtherWorkers <- Some(events.collect {
                                        case (worker, _, event: DiagnosticEvent.LeaseAcquired) if worker != "worker1" =>
                                          event.shardId
                                      }).filter(_.nonEmpty)
          } yield acquiredByWorker1.toSet subsetOf acquiredByOtherWorkers.toSet
        }.getOrElse(false)

        withRandomStreamAndApplicationName(nrShards) { (streamName, applicationName) =>
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
                                  .flatMap(time => events.update(_ :+ ((workerId, time, event))))
                                  .provideLayer(Clock.live) *> events.get.flatMap(events =>
                                  done.succeed(()).when(testIsComplete(events))
                                )

            _ <- ZManaged.finalizer {
                   events.get
                     .map(
                       _.filterNot(_._3.isInstanceOf[PollComplete])
                         .filterNot(_._3.isInstanceOf[DiagnosticEvent.Checkpoint])
                     )
                     .tap(allEvents => UIO(println(allEvents.mkString("\n"))))
                 }.use_ {
                   for {

                     stream <- ZStream
                                 .mergeAll(3)(
                                   // The zombie consumer: not updating checkpoints and not renewing leases
                                   consumer(
                                     streamName,
                                     applicationName,
                                     "worker1",
                                     handleEvent("worker1"),
                                     checkpointInterval = 5.minutes,
                                     renewInterval = 5.minutes
                                   ),
                                   ZStream.fromEffect(ZIO.sleep(5.seconds)) *> // Give worker1 the first lease
                                     consumer(streamName, applicationName, "worker2", handleEvent("worker2"))
                                       .ensuringFirst(log.warn("worker2 DONE")),
                                   ZStream.fromEffect(ZIO.sleep(5.seconds)) *> // Give worker1 the first lease
                                     consumer(streamName, applicationName, "worker3", handleEvent("worker3"))
                                       .ensuringFirst(log.warn("Worker3 DONE"))
                                 )
                                 .runDrain
                                 .fork
                     _      <- done.await
                     _      <- putStrLn("Interrupting producer and stream").orDie
                     _      <- producer.interrupt
                     _      <- stream.interrupt
                   } yield ()
                 }
          } yield assertCompletes
        }
      },
      testM("a worker must pick up an ended shard stream") {
        val nrRecords = 20000
        val nrShards  = 3

        withRandomStreamAndApplicationName(nrShards) { (streamName, applicationName) =>
          def consumer(workerId: String, emitDiagnostic: DiagnosticEvent => UIO[Unit]) =
            Consumer
              .shardedStream(
                streamName,
                applicationName,
                Serde.asciiString,
                workerIdentifier = workerId,
                leaseCoordinationSettings = LeaseCoordinationSettings(
                  renewInterval = 3.seconds,
                  refreshAndTakeInterval = 5.seconds,
                  maxParallelLeaseAcquisitions = 1
                ),
                emitDiagnostic = emitDiagnostic
              )
              .flatMapPar(Int.MaxValue) { case (shard @ _, shardStream, checkpointer) =>
                val out = shardStream
                  .tap(checkpointer.stage)
                  .aggregateAsyncWithin(ZTransducer.collectAllN(200), Schedule.fixed(1.second))
                  .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
                  .map(_.last)
                  .tap(_ => checkpointer.checkpoint())
                  .catchAll {
                    case Right(_) =>
                      ZStream.empty
                    case Left(e)  => ZStream.fail(e)
                  }

                if (shard == "shardId-000000000001") out.take(3) else out
              }

          // TODO extract DiagnosticEventList { def eventsByWorker(workerId), eventsAfter(instant), etc }
          for {
            producer      <- produceSampleRecords(streamName, nrRecords, chunkSize = 50, throttle = Some(1.second)).fork
            done          <- Promise.make[Nothing, Unit]
            events        <- Ref.make[List[DiagnosticEvent]](List.empty)
            emitDiagnostic = (workerId: String) =>
                               (event: DiagnosticEvent) =>
                                 onDiagnostic(workerId)(event) *>
                                   events.update(_ :+ event) *>
                                   done
                                     .succeed(())
                                     .whenM(events.get.map(_.collect { case _: DiagnosticEvent.LeaseAcquired =>
                                       1
                                     }.sum == nrShards + 1))

            stream <- consumer("worker1", emitDiagnostic("worker1")).runDrain.fork
            _      <- done.await
            _      <- producer.interrupt
            _      <- stream.interrupt
          } yield assertCompletes
        }
      },
      testM("must checkpoint when a shard ends") {
        val nrRecords = 20000
        val nrShards  = 3

        withRandomStreamAndApplicationName(nrShards) { (streamName, applicationName) =>
          def consumer(workerId: String, emitDiagnostic: DiagnosticEvent => UIO[Unit]) =
            Consumer
              .shardedStream(
                streamName,
                applicationName,
                Serde.asciiString,
                workerIdentifier = workerId,
                leaseCoordinationSettings = LeaseCoordinationSettings(
                  renewInterval = 30.seconds,
                  refreshAndTakeInterval = 10.seconds,
                  maxParallelLeaseAcquisitions = 1
                ),
                emitDiagnostic = emitDiagnostic
              )
              .mapM { case (shard @ _, shardStream, checkpointer) =>
                shardStream
                  .tap(checkpointer.stage)
                  .aggregateAsyncWithin(ZTransducer.collectAllN(200), Schedule.fixed(1.second))
                  .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
                  .map(_.last)
                  .tap(_ => checkpointer.checkpoint())
                  .catchAll {
                    case Right(_) =>
                      ZStream.empty
                    case Left(e)  => ZStream.fail(e)
                  }
                  .runDrain
                  .as(shard)
              }
              .take(1)

          for {
            producer      <- produceSampleRecords(streamName, nrRecords, chunkSize = 50, throttle = Some(1.second)).fork
            stream        <-
              consumer("worker1", e => UIO(println(e.toString))).runCollect.tapCause(e => UIO(println(e))).fork
            _             <- ZIO.sleep(20.seconds)
            _              = println("Resharding")
            _             <- kinesis
                               .updateShardCount(UpdateShardCountRequest(streamName, 4, ScalingType.UNIFORM_SCALING))
                               .mapError(_.toThrowable)
            finishedShard <- stream.join.map(_.head)
            _             <- producer.interrupt
            checkpoints   <- getCheckpoints(applicationName)
          } yield assert(checkpoints(finishedShard))(equalTo("SHARD_END"))
        }
      } @@ TestAspect.ifEnvSet("ENABLE_AWS"),
      testM("must not resume leases for ended shards") {
        val nrRecords = 20000
        val nrShards  = 3

        withRandomStreamAndApplicationName(nrShards) { (streamName, applicationName) =>
          def consumer(workerId: String, emitDiagnostic: DiagnosticEvent => UIO[Unit]) =
            Consumer
              .shardedStream(
                streamName,
                applicationName,
                Serde.asciiString,
                workerIdentifier = workerId,
                leaseCoordinationSettings = LeaseCoordinationSettings(
                  renewInterval = 30.seconds,
                  refreshAndTakeInterval = 10.seconds,
                  maxParallelLeaseAcquisitions = 1
                ),
                emitDiagnostic = emitDiagnostic
              )
              .mapMParUnordered(100) { case (shard @ _, shardStream, checkpointer) =>
                shardStream
                  .tap(checkpointer.stage)
                  .aggregateAsyncWithin(ZTransducer.collectAllN(200), Schedule.fixed(1.second))
                  .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
                  .map(_.last)
                  .tap(_ => checkpointer.checkpoint())
                  .catchAll {
                    case Right(_) =>
                      ZStream.empty
                    case Left(e)  => ZStream.fail(e)
                  }
                  .runDrain
                  .as(shard)
              }
              .take(3)

          for {
            producer <- produceSampleRecords(streamName, nrRecords, chunkSize = 50, throttle = Some(1.second)).fork

            stream  <-
              consumer("worker1", e => UIO(println(e.toString))).runCollect.tapCause(e => UIO(println(e))).fork
            _       <- ZIO.sleep(10.seconds)
            _        = println("Resharding")
            _       <- kinesis
                         .updateShardCount(UpdateShardCountRequest(streamName, nrShards * 2, ScalingType.UNIFORM_SCALING))
                         .mapError(_.toThrowable)
            _       <- ZIO.sleep(10.seconds)
            _       <- stream.join
            shards  <- TestUtil.getShards(streamName)
            _        = println(shards.mkString(", "))
            stream2 <-
              consumer("worker1", e => UIO(println(e.toString))).runCollect.tapCause(e => UIO(println(e))).fork
            _       <- ZIO.sleep(30.seconds)
            _       <- stream2.interrupt
            _       <- producer.interrupt
          } yield assertCompletes
        }
      } @@ TestAspect.ifEnvSet("ENABLE_AWS"),
      testM("parse aggregated records") {
        val nrShards  = 1
        val nrRecords = 10

        withRandomStreamAndApplicationName(nrShards) { (streamName, applicationName) =>
          val consumer =
            Consumer
              .shardedStream(
                streamName,
                applicationName,
                Serde.asciiString,
                emitDiagnostic = e => UIO(println(e.toString))
              )
              .flatMapPar(nrShards) { case (shard @ _, shardStream, checkpointer @ _) => shardStream }
              .take(nrRecords.toLong)
          for {
            _       <- produceSampleRecords(streamName, nrRecords, aggregated = true)
            records <- consumer.runCollect
          } yield assert(records)(hasSize(equalTo(nrRecords))) &&
            assert(records.flatMap(_.subSequenceNumber.toList).map(_.toInt).toList)(
              equalTo((0 until nrRecords).toList)
            ) &&
            assert(records.map(_.aggregated))(forall(isTrue))
        }
      },
      testM("resume at a subsequence number") {
        val nrShards  = 1
        val nrRecords = 10

        withRandomStreamAndApplicationName(nrShards) { (streamName, applicationName) =>
          def consume(nr: Int) =
            Consumer
              .shardedStream(
                streamName,
                applicationName,
                Serde.asciiString,
                emitDiagnostic = e => UIO(println(e.toString))
              )
              .mapMParUnordered(nrShards) { case (shard @ _, shardStream, checkpointer @ _) =>
                shardStream
                  .tap(checkpointer.stage)
                  .take(nr.toLong)
                  .runCollect
              }
              .flattenChunks
              .take(nr.toLong)

          for {
            _        <- produceSampleRecords(streamName, nrRecords, aggregated = true)
            records1 <- consume(5).runCollect
            _        <- putStrLn(records1.mkString("\n")).orDie
            records2 <- consume(5).runCollect
            _        <- putStrLn(records2.mkString("\n")).orDie
            records   = records1 ++ records2
          } yield assert(records)(hasSize(equalTo(nrRecords))) &&
            assert(records.flatMap(_.subSequenceNumber.toList).map(_.toInt).toList)(
              equalTo((0 until nrRecords).toList)
            )
        }
      }
    ).provideCustomLayerShared(env) @@
      TestAspect.timed @@
//      TestAspect.sequential @@ // For CircleCI
//      TestAspect.nonFlaky(10)
      TestAspect.timeoutWarning(45.seconds) @@
      TestAspect.timeout(120.seconds)

  val loggingLayer: ZLayer[Any, Nothing, Logging] =
    (Console.live ++ Clock.live) >>> Logging.console() >>> Logging.withRootLoggerName(getClass.getName)

  val useAws = Runtime.default.unsafeRun(system.envOrElse("ENABLE_AWS", "0")).toInt == 1

  val awsLayer: ZLayer[Any, Throwable, CloudWatch with Kinesis with DynamoDb] =
    if (useAws) client.defaultAwsLayer else LocalStackServices.localStackAwsLayer()

  val env: ZLayer[
    Any,
    Nothing,
    Kinesis with CloudWatch with DynamoDb with LeaseRepository with Clock with Logging
  ] =
    awsLayer.orDie >+>
      (DynamoDbLeaseRepository.live ++
        Clock.live ++ loggingLayer)

  def produceSampleRecords(
    streamName: String,
    nrRecords: Int,
    chunkSize: Int = 100,
    throttle: Option[Duration] = None,
    indexStart: Int = 1,
    aggregated: Boolean = false
  ): ZIO[Kinesis with Clock with Logging with Blocking, Throwable, Chunk[ProduceResponse]] =
    Producer
      .make(streamName, Serde.asciiString, ProducerSettings(maxParallelRequests = 1, aggregate = aggregated))
      .use { producer =>
        val records =
          (indexStart until (nrRecords + indexStart)).map(i => ProducerRecord(s"key$i", s"msg$i"))
        ZStream
          .fromIterable(records)
          .chunkN(chunkSize)
          .mapChunksM { chunk =>
            producer
              .produceChunk(chunk)
              .tapError(e => putStrLn(s"Error in producing fiber: $e").provideLayer(Console.live).orDie)
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
  ): ZIO[Kinesis with Clock with Logging with Blocking, Throwable, Chunk[ProduceResponse]] =
    Producer.make(streamName, Serde.asciiString).use { producer =>
      val records =
        (indexStart until (nrRecords + indexStart)).map(i => ProducerRecord(s"key$i", s"msg$i"))
      ZStream
        .fromIterable(records)
        .chunkN(chunkSize)
        .mapChunksM(chunk =>
          producer
            .produceChunk(chunk)
            .tapError(e => putStrLn(s"Error in producing fiber: $e").provideLayer(Console.live).orDie)
            .retry(retryOnResourceNotFound)
            .fork
            .map(fib => Chunk.single(fib))
        )
        .mapMPar(24)(_.join)
        .mapConcatChunk(Chunk.fromIterable)
        .runCollect
        .map(Chunk.fromIterable)
    }

  def onDiagnostic(worker: String): DiagnosticEvent => UIO[Unit] = {
    case _: PollComplete => UIO.unit
    case ev              => log.info(s"${worker}: ${ev}").provideLayer(loggingLayer)
  }

  def assertAllLeasesReleased(applicationName: String) =
    for {
      table  <- ZIO.service[LeaseRepository.Service]
      leases <- table.getLeases(applicationName).runCollect
    } yield assert(leases)(forall(hasField("owner", _.owner, isNone)))

  def getCheckpoints(
    applicationName: String
  ): ZIO[Clock with Has[LeaseRepository.Service], Throwable, Map[String, String]] =
    for {
      table      <- ZIO.service[LeaseRepository.Service]
      leases     <- table.getLeases(applicationName).runCollect
      checkpoints = leases.collect {
                      case l if l.checkpoint.isDefined =>
                        l.key -> (l.checkpoint.get match {
                          case Left(s @ _)  => s.stringValue
                          case Right(seqnr) => seqnr.sequenceNumber
                        })
                    }.toMap
    } yield checkpoints

  def deleteTable(tableName: String) =
    ZIO
      .service[LeaseRepository.Service]
      .flatMap(_.deleteTable(tableName).unit)

  def withRandomStreamAndApplicationName[R, A](nrShards: Int)(
    f: (String, String) => ZIO[R, Throwable, A]
  ): ZIO[
    Kinesis
      with Clock
      with Console
      with Logging
      with Console
      with Has[
        LeaseRepository.Service
      ]
      with R,
    Throwable,
    A
  ] =
    ZIO.effectTotal((streamPrefix + "testStream", streamPrefix + "testApplication")).flatMap {
      case (streamName, applicationName) =>
        withStream(streamName, shards = nrShards) {
          ZManaged.finalizer(deleteTable(applicationName).ignore).use_ { // Table may not have been created
            f(streamName, applicationName)
          }
        }
    }

}
