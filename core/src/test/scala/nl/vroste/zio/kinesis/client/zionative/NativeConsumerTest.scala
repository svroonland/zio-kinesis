package nl.vroste.zio.kinesis.client.zionative

import nl.vroste.zio.kinesis.client
import nl.vroste.zio.kinesis.client.Producer.ProduceResponse
import nl.vroste.zio.kinesis.client.TestUtil.{ retryOnResourceNotFound, withStream }
import nl.vroste.zio.kinesis.client._
import nl.vroste.zio.kinesis.client.localstack.LocalStackServices
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.zionative.DiagnosticEvent.{ LeaseAcquired, LeaseReleased, PollComplete }
import nl.vroste.zio.kinesis.client.zionative.leasecoordinator.LeaseCoordinationSettings
import nl.vroste.zio.kinesis.client.zionative.leaserepository.DynamoDbLeaseRepository
import zio.Console._
import zio.aws.cloudwatch.CloudWatch
import zio.aws.dynamodb.DynamoDb
import zio.aws.kinesis.Kinesis
import zio.aws.kinesis.model.primitives.{ PositiveIntegerObject, StreamName }
import zio.aws.kinesis.model.{ DescribeStreamRequest, ScalingType, UpdateShardCountRequest }
import zio.stream.{ ZSink, ZStream }
import zio.test.Assertion._
import zio.test._
import zio.{ System, _ }
import nl.vroste.zio.kinesis.client.zionative.LeaseRepository.Lease

import scala.collection.compat._
import java.time.Instant
import java.{ util => ju }
import zio.aws.kinesis.model.StreamStatus

object NativeConsumerTest extends ZIOSpecDefault {
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
      test("retrieve records from all shards") {
        val nrRecords = 2000
        val nrShards  = 1

        withRandomStreamAndApplicationName(nrShards) { (streamName, applicationName) =>
          for {
            _        <- ZIO.logInfo("Starting producer")
            producer <- produceSampleRecords(streamName, nrRecords, chunkSize = 500).fork
            _        <- ZIO.logInfo("Starting consumer")
            records  <- Consumer
                          .shardedStream(
                            streamName,
                            applicationName,
                            Serde.asciiString,
                            fetchMode = FetchMode.Polling(batchSize = 1000),
                            emitDiagnostic = onDiagnostic("worker1")
                          )
                          .flatMapPar(Int.MaxValue) { case (shard @ _, shardStream, checkpointer @ _) =>
                            shardStream
                              .tap(checkpointer.stage)
                          }
                          .take(nrRecords.toLong)
                          .zipWithIndex
                          .map(_._1)
                          .runCollect
            _        <- producer.interrupt
            shardIds <- Kinesis
                          .describeStream(DescribeStreamRequest(StreamName(streamName)))
                          .mapError(_.toThrowable)
                          .map(_.streamDescription.shards.map(_.shardId))

          } yield assert(records.map(_.shardId).toSet)(equalTo(shardIds.map(_.toString).toSet))
        }
      },
      test("release leases at shutdown") {
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
      test("checkpoint the last staged record at shutdown") {
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
      test("continue from the next message after the last checkpoint") {
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
                     shardStream
                       .tap(r => checkpointer.stage(r))
                       .aggregateAsyncWithin(ZSink.collectAllN[Record[String]](50), Schedule.fixed(5.minutes))
                       .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
                       .tap(_ => checkpointer.checkpoint())
                       .catchAll {
                         case Right(_) =>
                           ZStream.empty
                         case Left(e)  => ZStream.fail(e)
                       }
                       .flattenChunks
                   }
                   .take(nrRecords.toLong)
                   .runDrain
            _ <- producer.join

            indexOffset = nrRecords + 3
            _          <- produceSampleRecords(streamName, 1, indexStart = indexOffset) // Something arbitrary

            // TODO improve test: this could also pass with shard iterator LATEST or something
            firstRecord <- Consumer
                             .shardedStream(
                               streamName,
                               applicationName,
                               Serde.asciiString,
                               workerIdentifier = "worker2",
                               emitDiagnostic = onDiagnostic("worker2")
                             )
                             .flatMapPar(Int.MaxValue) { case (shard @ _, shardStream, checkpointer @ _) =>
                               shardStream
                             }
                             .take(1)
                             .runHead

          } yield assert(firstRecord)(
            isSome(hasField("key", (_: Record[String]).partitionKey, equalTo(s"key${indexOffset}")))
          )
        }
      },
      test("worker steals leases from other worker until they both have an equal share") {
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
                                      // .tap(r => ZIO.logDebug(s"Worker 1 got record on shard ${r.shardId}"))
                                      .tap(checkpointer.stage)
                                      .tap(_ => consumer1Started.succeed(()))
                                      .aggregateAsyncWithin(
                                        ZSink.collectAllN[Record[String]](2000),
                                        Schedule.fixed(5.minutes)
                                      )
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
//                            .updateService[Logger[String]](_.named("worker1"))
            consumer2         = Consumer
                                  .shardedStream(
                                    streamName,
                                    applicationName,
                                    Serde.asciiString,
                                    workerIdentifier = "worker2",
                                    emitDiagnostic = onDiagnostic("worker2")
                                  )
                                  .tap(tp => ZIO.logInfo(s"Got tuple ${tp}"))
                                  .take(2) // 5 shards, so we expect 2
//                            .updateService[Logger[String]](_.named("worker2"))
            worker1          <- consumer1.runDrain.tapError(e => ZIO.logError(s"Worker1 failed: ${e}")).fork
            _                <- consumer1Started.await
            _                <- ZIO.logInfo("Consumer 1 has started, starting consumer 2")
            _                <- consumer2.runDrain
            _                <- ZIO.logInfo("Shutting down worker 1")
            _                <- worker1.interrupt
            _                <- ZIO.logInfo("Shutting down producer")
            _                <- producer.interrupt

          } yield assertCompletes
        }
      },
      test("workers should be able to start concurrently and both get some shards") {
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
                              .aggregateAsyncWithin(
                                ZSink.collectAllN[Record[String]](200),
                                Schedule.fixed(1.second)
                              )
                              .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
                              // .tap(_ => checkpointer.checkpoint())
                              .catchAll {
                                case Right(_) =>
                                  ZStream.empty
                                case Left(e)  => ZStream.fail(e)
                              }
                              .ensuring(ZIO.logDebug(s"Shard stream worker 1 ${shard} completed"))
                          }
                          .tap(_ => ZIO.logInfo("WORKER1 GOT A BATCH"))
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
                            ZStream.fromZIO(shardsProcessedByConsumer2.update(_ + shard)) *>
                              shardStream
                                .tap(checkpointer.stage)
                                .aggregateAsyncWithin(
                                  ZSink.collectAllN[Record[String]](200),
                                  Schedule.fixed(1.second)
                                )
                                .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
                                // .tap(_ => checkpointer.checkpoint())
                                .catchAll {
                                  case Right(_) =>
                                    ZStream.empty
                                  case Left(e)  => ZStream.fail(e)
                                }
                                .ensuring(ZIO.logDebug(s"Shard stream worker 2 ${shard} completed"))
                          }
                          .tap(_ => ZIO.logInfo("WORKER2 GOT A BATCH"))
                          .take(10)

            _ <- consumer1.merge(consumer2).runCollect
            _ <- producer.interrupt
          } yield assertCompletes
        }
      },
      test("workers must take over from a stopped consumer") {
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
                .aggregateAsyncWithin(ZSink.collectAllN[Record[String]](200), Schedule.fixed(1.second))
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
              ZStream.unwrap(ZIO.logError(e.toString).as(ZStream.fail(e)))
            }
//            .updateService[Logger[String]](_.named(s"worker-${workerId}"))

        withRandomStreamAndApplicationName(nrShards) { (streamName, applicationName) =>
          for {
            consumer1Started <- Promise.make[Throwable, Unit]
            consumer2Started <- Promise.make[Throwable, Unit]
            consumer3Started <- Promise.make[Throwable, Unit]
            producer         <-
              produceSampleRecords(streamName, nrRecords, chunkSize = 50, throttle = Some(1.second)).forkScoped
            events           <- Ref.make[List[(String, Instant, DiagnosticEvent)]](List.empty)
            emitDiagnostic    = (workerId: String) =>
                                  (event: DiagnosticEvent) =>
                                    onDiagnostic(workerId)(event) *>
                                      zio.Clock.currentDateTime
                                        .map(_.toInstant())
                                        .flatMap(time => events.update(_ :+ ((workerId, time, event))))

            waitForAllLeasesReacquired = {
              def getCurrentHeldLeases = for {
                allEvents <- events.get
                heldLeases = allEvents.foldLeft(Set.empty[String]) { case (acc, (_, _, event)) =>
                               event match {
                                 case LeaseAcquired(shardId, _) => acc + shardId
                                 case LeaseReleased(shardId)    => acc - shardId
                                 case _                         => acc
                               }
                             }
              } yield heldLeases

              getCurrentHeldLeases.delay(100.millis).repeatUntil(_.size == nrShards)
            }

            _ <- {
                   for {

                     worker1 <- (consumer(streamName, applicationName, "worker1", emitDiagnostic("worker1"))
                                  .tap(_ => consumer1Started.succeed(()))
                                  .runDrain
                                  .tapError(e => ZIO.logError(s"Worker1 failed with error: ${e}"))
                                  *> ZIO.logWarning("worker1 done")).forkScoped

                     worker2 <- consumer(streamName, applicationName, "worker2", emitDiagnostic("worker2"))
                                  .ensuring(
                                    ZIO.logWarning("worker2 DONE")
                                  )
                                  .tap(_ => consumer2Started.succeed(()))
                                  .runDrain
                                  .forkScoped
                     worker3 <- consumer(streamName, applicationName, "worker3", emitDiagnostic("worker3"))
                                  .ensuring(
                                    ZIO.logWarning("worker3 DONE")
                                  )
                                  .tap(_ => consumer3Started.succeed(()))
                                  .runDrain
                                  .forkScoped

                     _ <- consumer1Started.await *> consumer2Started.await *> consumer3Started.await
                     _ <- ZIO.logDebug("All consumers have started, interrupting consumer 1")
                     _ <- worker1.interrupt
                     _ <- waitForAllLeasesReacquired
                     _ <- ZIO.logDebug("Interrupting producer")
                     _ <- producer.interrupt
                     _ <- ZIO.logDebug("Interrupting streams")
                     _ <- (worker2.interrupt.tap(_ => ZIO.logInfo("Done interrupting worker 2"))) *>
                            worker3.interrupt.tap(_ => ZIO.logInfo("Done interrupting worker 3"))
                   } yield ()
                 }.onInterrupt(
                   events.get
                     .map(
                       _.filterNot(_._3.isInstanceOf[PollComplete])
                         .filterNot(_._3.isInstanceOf[DiagnosticEvent.Checkpoint])
                     )
                     .tap(allEvents => ZIO.logDebug(allEvents.mkString("\n")))
                 )

          } yield assertCompletes
        }
      },
      test("workers must take over from a zombie consumer") {
        val nrRecords = 2000
        val nrShards  = 7

        def consumer(
          streamName: String,
          applicationName: String,
          workerId: String,
          emitDiagnostic: DiagnosticEvent => UIO[Unit],
          onStarted: UIO[Any] = ZIO.unit,
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
              emitDiagnostic = emitDiagnostic,
              shardAssignmentStrategy = ShardAssignmentStrategy.balanced(renewInterval + 1.second)
            )
            .flatMapPar(Int.MaxValue) { case (shard @ _, shardStream, checkpointer) =>
              shardStream
                .tap(_ => onStarted)
                .tap(checkpointer.stage)
                .aggregateAsyncWithin(ZSink.collectAllN[Record[String]](200000), Schedule.fixed(checkpointInterval))
                .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
                .mapConcat(_.lastOption)
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

        def testIsComplete(events: List[(String, Instant, DiagnosticEvent)]) = {
          def getCurrentHeldLeasesByWorker =
            events.foldLeft(Map.empty[String, Set[String]]) { case (acc, (worker, _, event)) =>
              event match {
                case LeaseAcquired(shardId, _) => acc + (worker -> (acc.getOrElse(worker, Set.empty) + shardId))
                case LeaseReleased(shardId)    => acc + (worker -> (acc.getOrElse(worker, Set.empty) - shardId))
                case _                         => acc
              }
            }

          val leasesByAliveWorkers = getCurrentHeldLeasesByWorker.collect {
            case (worker, leases) if worker != "worker1" => leases
          }.flatten

          leasesByAliveWorkers.size == nrShards
        }

        withRandomStreamAndApplicationName(nrShards) { (streamName, applicationName) =>
          for {
            _          <- produceSampleRecords(streamName, nrRecords, chunkSize = 50).fork
            done       <- Promise.make[Nothing, Unit]
            events     <- Ref.make[List[(String, Instant, DiagnosticEvent)]](List.empty)
            handleEvent = (workerId: String) =>
                            (event: DiagnosticEvent) =>
                              onDiagnostic(workerId)(event) *>
                                zio.Clock.currentDateTime
                                  .map(_.toInstant())
                                  .flatMap(time => events.update(_ :+ ((workerId, time, event)))) *>
                                events.get
                                  .flatMap(events => done.succeed(()).when(testIsComplete(events)))
                                  .unit

            consumer1Started <- Promise.make[Throwable, Unit]

            _ <- (
                   ZStream
                     .mergeAll(3)(
                       // The zombie consumer: not updating checkpoints and not renewing leases
                       consumer(
                         streamName,
                         applicationName,
                         "worker1",
                         handleEvent("worker1"),
                         consumer1Started.succeed(()),
                         checkpointInterval = 5.minutes,
                         renewInterval = 5.minutes // Such that it appears to be dead
                       ),
                       ZStream.fromZIO(consumer1Started.await) *> // Give worker1 the first lease
                         consumer(streamName, applicationName, "worker2", handleEvent("worker2"))
                           .ensuring(ZIO.logWarning("worker2 DONE")),
                       ZStream.fromZIO(consumer1Started.await) *> // Give worker1 the first lease
                         consumer(streamName, applicationName, "worker3", handleEvent("worker3"))
                           .ensuring(ZIO.logWarning("Worker3 DONE"))
                     )
                     .runDrain
                     .tapErrorCause(c => ZIO.logError(s"${c.prettyPrint}"))
                   )
                   .withFinalizer { _ =>
                     events.get
                       .map(
                         _.filterNot(_._3.isInstanceOf[PollComplete])
                           .filterNot(_._3.isInstanceOf[DiagnosticEvent.Checkpoint])
                       )
                       .tap(allEvents => ZIO.logDebug(allEvents.mkString("\n")))
                   }
                   .disconnect raceFirst done.await
          } yield assertCompletes
        }
      },
      test("a worker must pick up an ended shard stream") {
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
                  .aggregateAsyncWithin(ZSink.collectAllN[Record[String]](200), Schedule.fixed(1.second))
                  .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
                  .mapConcat(_.lastOption)
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
            _ <- produceSampleRecords(streamName, nrRecords, chunkSize = 50, throttle = Some(1.second)).fork

            done          <- Promise.make[Nothing, Unit]
            events        <- Ref.make[List[DiagnosticEvent]](List.empty)
            emitDiagnostic = (workerId: String) =>
                               (event: DiagnosticEvent) =>
                                 onDiagnostic(workerId)(event) *>
                                   events.update(_ :+ event) *>
                                   done
                                     .succeed(())
                                     .whenZIO(events.get.map(_.collect { case _: DiagnosticEvent.LeaseAcquired =>
                                       1
                                     }.sum == nrShards + 1))
                                     .unit

            _ <- done.await raceFirst consumer("worker1", emitDiagnostic("worker1")).runDrain
          } yield assertCompletes
        }
      },
      test("must checkpoint when a shard ends") {
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
              .mapZIO { case (shard @ _, shardStream, checkpointer) =>
                shardStream
                  .tap(checkpointer.stage)
                  .aggregateAsyncWithin(ZSink.collectAllN[Record[String]](200), Schedule.fixed(1.second))
                  .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
                  .mapConcat(_.lastOption)
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
              consumer("worker1", e => ZIO.logDebug(e.toString)).runCollect
                .tapErrorCause(ZIO.logErrorCause(_))
                .fork
            _             <- ZIO.sleep(20.seconds)
            _              = println("Resharding")
            _             <- Kinesis
                               .updateShardCount(
                                 UpdateShardCountRequest(
                                   StreamName(streamName),
                                   PositiveIntegerObject(4),
                                   ScalingType.UNIFORM_SCALING
                                 )
                               )
                               .mapError(_.toThrowable)
            finishedShard <- stream.join.map(_.head)
            _             <- producer.interrupt
            checkpoints   <- getCheckpoints(applicationName)
          } yield assert(checkpoints(finishedShard))(equalTo("SHARD_END"))
        }
      } @@ TestAspect.ifEnvSet("ENABLE_AWS"),
      test("must not resume leases for ended shards") {
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
              .mapZIOParUnordered(100) { case (shard @ _, shardStream, checkpointer) =>
                shardStream
                  .tap(checkpointer.stage)
                  .aggregateAsyncWithin(ZSink.collectAllN[Record[String]](200), Schedule.fixed(1.second))
                  .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
                  .mapConcat(_.lastOption)
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
              consumer("worker1", e => ZIO.logDebug(e.toString)).runCollect
                .tapErrorCause(ZIO.logErrorCause(_))
                .fork
            _       <- ZIO.sleep(10.seconds)
            _        = println("Resharding")
            _       <- Kinesis
                         .updateShardCount(
                           UpdateShardCountRequest(
                             StreamName(streamName),
                             PositiveIntegerObject(nrShards * 2),
                             ScalingType.UNIFORM_SCALING
                           )
                         )
                         .mapError(_.toThrowable)
            _       <- ZIO.sleep(10.seconds)
            _       <- stream.join
            shards  <- TestUtil.getShards(streamName)
            _        = println(shards.mkString(", "))
            stream2 <-
              consumer("worker1", e => ZIO.logDebug(e.toString)).runCollect
                .tapErrorCause(ZIO.logErrorCause(_))
                .fork
            _       <- ZIO.sleep(30.seconds)
            _       <- stream2.interrupt
            _       <- producer.interrupt
          } yield assertCompletes
        }
      } @@ TestAspect.ifEnvSet("ENABLE_AWS"),
      test("parse aggregated records") {
        val nrShards  = 1
        val nrRecords = 10

        withRandomStreamAndApplicationName(nrShards) { (streamName, applicationName) =>
          val consumer =
            Consumer
              .shardedStream(
                streamName,
                applicationName,
                Serde.asciiString,
                emitDiagnostic = e => ZIO.logDebug(e.toString)
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
      test("resume at a subsequence number") {
        val nrShards  = 1
        val nrRecords = 10

        withRandomStreamAndApplicationName(nrShards) { (streamName, applicationName) =>
          def consume(nr: Int) =
            Consumer
              .shardedStream(
                streamName,
                applicationName,
                Serde.asciiString,
                emitDiagnostic = e => ZIO.logDebug(e.toString)
              )
              .flatMapPar(nrShards, 1) { case (shard @ _, shardStream, checkpointer @ _) =>
                shardStream.map((_, checkpointer))
              }
              .take(nr.toLong)
              .debug
              .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
              .tap { case (r, checkpointer) => checkpointer.checkpointNow[Any](r) }
              .catchAll {
                case Right(_) =>
                  ZStream.empty
                case Left(e)  => ZStream.fail(e)
              }
              .map(_._1)

          for {
            _        <- produceSampleRecords(streamName, nrRecords, aggregated = true)
            records1 <- consume(5).runCollect
            _        <- printLine(records1.mkString("\n")).orDie
            records2 <- consume(5).runCollect
            _        <- printLine(records2.mkString("\n")).orDie
            records   = records1 ++ records2
          } yield assert(records)(hasSize(equalTo(nrRecords))) &&
            assert(records.flatMap(_.subSequenceNumber.toList).map(_.toInt).toList)(
              equalTo((0 until nrRecords).toList)
            )
        }
      },
      test("regression: correctly switch from parent to child when parent is empty") {
        val nrShards             = 1
        val nrShardsAfterScaling = 2
        val nrRecords            = 10

        withRandomStreamAndApplicationName(nrShards) { (streamName, applicationName) =>
          def consume(nr: Int) =
            Consumer
              .shardedStream(
                streamName,
                applicationName,
                Serde.asciiString,
                emitDiagnostic = e => ZIO.logDebug(e.toString)
              )
              .flatMapPar(nrShards, 1) { case (shard @ _, shardStream, checkpointer @ _) =>
                shardStream.map((_, checkpointer))
              }
              .take(nr.toLong)
              .debug
              .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
              .tap { case (r, checkpointer) => checkpointer.checkpointNow[Any](r) }
              .catchAll {
                case Right(_) =>
                  ZStream.empty
                case Left(e)  => ZStream.fail(e)
              }
              .map(_._1)

          for {
            _               <- produceSampleRecords(streamName, nrRecords, aggregated = true)
            _               <- consume(nrRecords).runCollect
            _               <- scaleStream(streamName, nrShardsAfterScaling)
            consumer        <- consume(1).runCollect.forkScoped
            parentsCompleted = getCheckpoints(applicationName).map(_.values.count(_ == "SHARD_END") == nrShards)
            _               <- parentsCompleted.delay(200.millis).repeatUntil(identity)
            _               <- consumer.interrupt
          } yield assertCompletes
        }
      } @@ awsOnly,
      test("regression: will not consume child shards while parent is still active") {
        val nrRecords           = 1
        val firstShard          = "shardId-000000000000"
        val checkpointsToSample = 200L

        withRandomStreamAndApplicationName(1) { (streamName, applicationName) =>
          def consume(nr: Int) =
            Consumer
              .shardedStream(
                streamName,
                applicationName,
                Serde.asciiString,
                emitDiagnostic = e => ZIO.logDebug(e.toString)
              )
              .flatMapPar(Int.MaxValue, 1) { case (shard @ _, shardStream, checkpointer @ _) =>
                shardStream.map((_, shard, checkpointer))
              }
              .take(nr.toLong)
              .debug
              .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
              .tap { case (r, shard, checkpointer) =>
                checkpointer.checkpointNow[Any](r).unless(shard == firstShard)
              }
              .catchAll {
                case Right(_) =>
                  ZStream.empty
                case Left(e)  => ZStream.fail(e)
              }
              .map(_._1)

          def shardHasBeenConsumed(lease: Lease) =
            lease.key == firstShard ||
              lease.owner.nonEmpty ||
              lease.checkpoint.exists(entry => entry == Left(SpecialCheckpoint.ShardEnd) || entry.isRight)

          for {
            _            <- produceSampleRecords(streamName, nrRecords, aggregated = true)
            _            <- consume(nrRecords).runCollect
            _            <- scaleStream(streamName, 2)
            consumer     <- consume(nrRecords + 1).runCollect.forkScoped
            activeShards <- ZStream
                              .repeatZIOWithSchedule(getLeases(applicationName).exit, Schedule.spaced(10.millis))
                              .collect { case Exit.Success(leases) => leases.count(shardHasBeenConsumed) }
                              .take(checkpointsToSample)
                              .runCollect
            _            <- consumer.interrupt
          } yield assert(activeShards)(forall(isLessThanEqualTo(1)))
        }
      } @@ awsOnly
    ).provideLayerShared(env) @@
      TestAspect.timed @@
      TestAspect.withLiveClock @@
      TestAspect.sequential @@ // For CircleCI
      TestAspect.timeoutWarning(45.seconds) @@
      TestAspect.timeout(120.seconds)

  val useAws = Unsafe.unsafe { implicit unsafe =>
    Runtime.default.unsafe.run(System.envOrElse("ENABLE_AWS", "0")).getOrThrow().toInt == 1
  }

  val awsLayer: ZLayer[Any, Throwable, CloudWatch with Kinesis with DynamoDb] =
    if (useAws) client.defaultAwsLayer else LocalStackServices.localStackAwsLayer()

  val awsOnly = if (useAws) TestAspect.identity else TestAspect.ignore

  val env: ZLayer[Any, Nothing, Scope with CloudWatch with Kinesis with DynamoDb with LeaseRepository] =
    Scope.default >+> awsLayer.orDie >+> DynamoDbLeaseRepository.live >+>
      Runtime.removeDefaultLoggers >+>
      Runtime.addLogger(ZLogger.default.map(println(_)).filterLogLevel(_ >= LogLevel.Debug))

  def produceSampleRecords(
    streamName: String,
    nrRecords: Int,
    chunkSize: Int = 100,
    throttle: Option[Duration] = None,
    indexStart: Int = 1,
    aggregated: Boolean = false
  ): ZIO[Kinesis with Scope, Throwable, Chunk[ProduceResponse]] = {
    val aggregation = if (aggregated)
      Producer.Aggregation.ByPredictedShard()
    else
      Producer.Aggregation.Disabled

    Producer
      .make(streamName, Serde.asciiString, ProducerSettings(maxParallelRequests = 1, aggregation = aggregation))
      .flatMap { producer =>
        val records =
          (indexStart until (nrRecords + indexStart)).map(i => ProducerRecord(s"key$i", s"msg$i"))
        ZStream
          .fromIterable(records)
          .rechunk(chunkSize)
          .mapChunksZIO { chunk =>
            producer
              .produceChunk(chunk)
              .tapError(e => printLine(s"Error in producing fiber: $e").orDie)
              .retry(retryOnResourceNotFound)
              .tap(_ => throttle.map(ZIO.sleep(_)).getOrElse(ZIO.unit))
              .map(Chunk.fromIterable)
          }
          .runCollect
          .map(Chunk.fromIterable)
      }
    }

  def produceSampleRecordsMassivelyParallel(
    streamName: String,
    nrRecords: Int,
    chunkSize: Int = 100,
    indexStart: Int = 1
  ): ZIO[Kinesis with Any, Throwable, Chunk[ProduceResponse]] = ZIO.scoped {
    Producer.make(streamName, Serde.asciiString).flatMap { producer =>
      val records =
        (indexStart until (nrRecords + indexStart)).map(i => ProducerRecord(s"key$i", s"msg$i"))
      ZStream
        .fromIterable(records)
        .rechunk(chunkSize)
        .mapChunksZIO(chunk =>
          producer
            .produceChunk(chunk)
            .tapError(e => printLine(s"Error in producing fiber: $e").orDie)
            .retry(retryOnResourceNotFound)
            .fork
            .map(fib => Chunk.single(fib))
        )
        .mapZIOPar(24)(_.join)
        .mapConcatChunk(Chunk.fromIterable)
        .runCollect
        .map(Chunk.fromIterable)
    }
  }

  def onDiagnostic(worker: String): DiagnosticEvent => UIO[Unit] = {
    case _: PollComplete => ZIO.unit
    case ev              => ZIO.logInfo(s"${worker}: ${ev}")
  }

  def assertAllLeasesReleased(applicationName: String) =
    for {
      table  <- ZIO.service[LeaseRepository]
      leases <- table.getLeases(applicationName).runCollect
    } yield assert(leases)(forall(hasField("owner", _.owner, isNone)))

  def getLeases(
    applicationName: String
  ): ZIO[LeaseRepository, Throwable, Chunk[Lease]] =
    for {
      table  <- ZIO.service[LeaseRepository]
      leases <- table.getLeases(applicationName).runCollect
    } yield leases

  def getCheckpoints(
    applicationName: String
  ): ZIO[LeaseRepository, Throwable, Map[String, String]] =
    for {
      leases     <- getLeases(applicationName)
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
      .service[LeaseRepository]
      .flatMap(_.deleteTable(tableName).unit)

  def scaleStream(streamName: String, desiredShardCount: Int) = {
    val isActive = Kinesis
      .describeStream(DescribeStreamRequest(StreamName(streamName)))
      .map(_.streamDescription.streamStatus == StreamStatus.ACTIVE)

    val awaitActive = ZIO.ifZIO(isActive)(ZIO.unit, isActive.delay(200.millis).repeatUntil(identity).unit)

    Kinesis
      .updateShardCount(
        UpdateShardCountRequest(
          StreamName(streamName),
          PositiveIntegerObject(desiredShardCount),
          ScalingType.UNIFORM_SCALING
        )
      ) *> awaitActive
  }.mapError(_.toThrowable)

  def withRandomStreamAndApplicationName[R, A](nrShards: Int)(
    f: (String, String) => ZIO[R, Throwable, A]
  ): ZIO[Kinesis with LeaseRepository with R, Throwable, A] =
    ZIO.succeed((streamPrefix + "testStream", streamPrefix + "testApplication")).flatMap {
      case (streamName, applicationName) =>
        withStream(streamName, shards = nrShards) {
          ZIO.scoped[R with LeaseRepository] {
            ZIO.addFinalizer(deleteTable(applicationName).ignore) *> { // Table may not have been created
              f(streamName, applicationName)
            }
          }
        }
    }

}
