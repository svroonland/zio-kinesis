package nl.vroste.zio.kinesis.client.dynamicconsumer

import io.github.vigoo.zioaws.cloudwatch.CloudWatch
import io.github.vigoo.zioaws.dynamodb.DynamoDb
import io.github.vigoo.zioaws.kinesis
import io.github.vigoo.zioaws.kinesis.model.ScalingType
import io.github.vigoo.zioaws.kinesis.{ model, Kinesis }
import nl.vroste.zio.kinesis.client
import nl.vroste.zio.kinesis.client.TestUtil
import nl.vroste.zio.kinesis.client.localstack.LocalStackServices
import nl.vroste.zio.kinesis.client.serde.Serde
import software.amazon.kinesis.exceptions.ShutdownException
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.{ putStrLn, Console }
import zio.duration.{ durationInt, Duration }
import zio.logging.{ LogLevel, Logging }
import zio.random.Random
import zio.stream.{ SubscriptionRef, ZStream, ZTransducer }
import zio.test.Assertion._
import zio.test.TestAspect.timeout
import zio.test._

object DynamicConsumerTest extends DefaultRunnableSpec {
  import TestUtil._

  val loggingLayer: ZLayer[Any, Nothing, Logging] =
    (Console.live ++ Clock.live) >>> Logging.console(LogLevel.Trace) >>> Logging.withRootLoggerName(getClass.getName)

  val useAws = Runtime.default.unsafeRun(system.envOrElse("ENABLE_AWS", "0")).toInt == 1

  private val env: ZLayer[
    Any,
    Throwable,
    CloudWatch with Kinesis with DynamoDb with Logging with DynamicConsumer with Clock with Blocking with Random with Console with system.System
  ] = (if (useAws) client.defaultAwsLayer else LocalStackServices.localStackAwsLayer()) >+> loggingLayer >+>
    (DynamicConsumer.live ++ Clock.live ++ Blocking.live ++ Random.live ++ Console.live ++ zio.system.System.live)

  def testConsume1 =
    testM("consume records produced on all shards produced on the stream") {
      withRandomStreamEnv(2) { (streamName, applicationName) =>
        for {
          _       <- putStrLn("Putting records").orDie
          _       <- TestUtil
                 .produceRecords(
                   streamName,
                   1000,
                   10,
                   10
                 )
                 .fork

          service <- ZIO.service[DynamicConsumer.Service]
          records <- service
                       .shardedStream(
                         streamName,
                         applicationName = applicationName,
                         deserializer = Serde.asciiString,
                         configureKcl = _.withPolling
                       )
                       .flatMapPar(Int.MaxValue) {
                         case (shardId @ _, shardStream, checkpointer) =>
                           shardStream.tap(r =>
                             putStrLn(s"Got record $r").orDie *> checkpointer
                               .checkpointNow(r)
                               .retry(Schedule.exponential(100.millis))
                           )
                       }
                       .take(2)
                       .runCollect

        } yield assert(records)(hasSize(equalTo(2)))
      }
    }

  def testConsume2 =
    testM("support multiple parallel consumers on the same Kinesis stream") {
      withRandomStreamEnv(10) { (streamName, applicationName) =>
        def streamConsumer(
          workerIdentifier: String,
          activeConsumers: RefM[Set[String]]
        ): ZStream[Console with Blocking with DynamicConsumer with Clock, Throwable, (String, String)] =
          for {
            service <- ZStream.service[DynamicConsumer.Service]
            stream  <- ZStream
                        .fromEffect(putStrLn(s"Starting consumer $workerIdentifier").orDie)
                        .flatMap(_ =>
                          service
                            .shardedStream(
                              streamName,
                              applicationName = applicationName,
                              deserializer = Serde.asciiString,
                              workerIdentifier = applicationName + "-" + workerIdentifier,
                              configureKcl = _.withPolling
                            )
                            .flatMapPar(Int.MaxValue) {
                              case (shardId, shardStream, checkpointer @ _) =>
                                shardStream
                                  .via(checkpointer.checkpointBatched[Blocking with Clock](1000, 1.second))
                                  .as((workerIdentifier, shardId))
                                  // Background and a bit delayed so we get a chance to actually emit some records
                                  .tap(_ => activeConsumers.update(s => ZIO(s + workerIdentifier)).delay(1.second).fork)
                                  .ensuring(putStrLn(s"Shard $shardId completed for consumer $workerIdentifier").orDie)
                                  .catchSome {
                                    case _: ShutdownException => // This will be thrown when the shard lease has been stolen
                                      // Abort the stream when we no longer have the lease
                                      ZStream.empty
                                  }
                            }
                        )

          } yield stream

        for {
          _                     <- putStrLn("Putting records").orDie
          _                     <- TestUtil.produceRecords(streamName, 20000, 80, 10).fork
          _                     <- putStrLn("Starting dynamic consumers").orDie
          activeConsumers       <- SubscriptionRef.make(Set.empty[String])
          allConsumersGotAShard <- activeConsumers.changes.takeUntil(_ == Set("1", "2")).runDrain.fork
          _                     <- (streamConsumer("1", activeConsumers.ref)
                   merge delayStream(streamConsumer("2", activeConsumers.ref), 5.seconds))
                 .interruptWhen(allConsumersGotAShard.join)
                 .runCollect
        } yield assertCompletes
      }
    }

  def testCheckpointAtShutdown =
    testM("checkpoint for the last processed record at stream shutdown") {
      withRandomStreamEnv(2) { (streamName, applicationName) =>
        def streamConsumer(
          interrupted: Promise[Nothing, Unit],
          nrRecordsSeen: Ref[Int],
          lastProcessedRecords: Ref[Map[String, String]],
          lastCheckpointedRecords: Ref[Map[String, String]]
        ): ZStream[
          Console with Blocking with Clock with DynamicConsumer with Kinesis,
          Throwable,
          DynamicConsumer.Record[
            String
          ]
        ] =
          (for {
            service <- ZStream.service[DynamicConsumer.Service]
            stream  <- service
                        .shardedStream(
                          streamName,
                          applicationName = applicationName,
                          deserializer = Serde.asciiString,
                          configureKcl = _.withPolling,
                          requestShutdown = interrupted.await *> UIO(println("Interrupting shardedStream"))
                        )
                        .flatMapPar(Int.MaxValue) {
                          case (shardId, shardStream, checkpointer @ _) =>
                            shardStream
                              .tap(record => lastProcessedRecords.update(_ + (shardId -> record.sequenceNumber)))
                              .tap(checkpointer.stage)
                              .tap(_ => nrRecordsSeen.update(_ + 1))
                              .tap(record =>
                                ZIO.whenM(nrRecordsSeen.get.map(_ == 500))(
                                  putStrLn(s"Interrupting for partition key ${record.partitionKey}").orDie
                                    *> interrupted.succeed(())
                                )
                              )
                              // It's important that the checkpointing is always done before flattening the stream, otherwise
                              // we cannot guarantee that the KCL has not yet shutdown the record processor and taken away the lease
                              .aggregateAsyncWithin(
                                ZTransducer.collectAllN(
                                  100
                                ), // TODO we need to make sure that in our test this thing has some records buffered after shutdown request
                                Schedule.fixed(1.seconds)
                              )
                              .mapConcat(_.toList)
                              .tap { r =>
                                (putStrLn(s"Shard ${r.shardId}: checkpointing for record $r $interrupted").orDie *>
                                  checkpointer.checkpoint)
                                  .unlessM(interrupted.isDone)
                                  .tapError(e => ZIO(println(s"Checkpointing failed: ${e}")))
                                  .tap(_ => lastCheckpointedRecords.update(_ + (shardId -> r.sequenceNumber)))
                                  .tap(_ => ZIO(println(s"Checkpointing for shard ${r.shardId} done")))
                              }
                        }
          } yield stream)

        for {
          _                         <- TestUtil
                 .produceRecords(streamName, 10000, 10, 10)
                 .tap(_ => ZIO(println("PRODUCING RECORDS DONE")))
                 .fork

          interrupted               <- Promise
                           .make[Nothing, Unit]
          nrRecordsSeen             <- Ref.make(0)
          lastProcessedRecords      <- Ref.make[Map[String, String]](Map.empty) // Shard -> Sequence Nr
          lastCheckpointedRecords   <- Ref.make[Map[String, String]](Map.empty) // Shard -> Sequence Nr
          consumer                  <-
            streamConsumer(interrupted, nrRecordsSeen, lastProcessedRecords, lastCheckpointedRecords).runCollect.fork
          _                         <- interrupted.await
          _                         <- consumer.join
          (processed, checkpointed) <- (lastProcessedRecords.get zip lastCheckpointedRecords.get)
        } yield assert(processed)(equalTo(checkpointed))
      }
    } @@ TestAspect.timeout(5.minutes)

  def testShardEnd =
    testM("checkpoint for the last processed record at shard end") {
      val nrShards = 2

      withRandomStreamEnv(nrShards) { (streamName, applicationName) =>
        def streamConsumer(
          requestShutdown: Promise[Nothing, Unit],
          firstRecordProcessed: Promise[Nothing, Unit],
          newShardDetected: Queue[Unit]
        ): ZStream[
          Console with Blocking with Clock with DynamicConsumer with Kinesis,
          Throwable,
          DynamicConsumer.Record[
            String
          ]
        ] =
          for {
            service <- ZStream.service[DynamicConsumer.Service]
            stream  <- service
                        .shardedStream(
                          streamName,
                          applicationName = applicationName,
                          deserializer = Serde.asciiString,
                          configureKcl = _.withPolling,
                          requestShutdown = requestShutdown.await *> UIO(println("Interrupting shardedStream"))
                        )
                        .tap(_ => newShardDetected.offer(()))
                        .flatMapPar(Int.MaxValue) {
                          case (shardId @ _, shardStream, checkpointer @ _) =>
                            shardStream
                              .tap(_ => firstRecordProcessed.succeed(()))
                              .tap(checkpointer.stage)
                              .aggregateAsyncWithin(
                                ZTransducer.last, // TODO we need to make sure that in our test this thing has some records buffered after shutdown request
                                Schedule.fixed(1.seconds)
                              )
                              .mapConcat(_.toList)
                              .tap { r =>
                                putStrLn(s"Shard ${r.shardId}: checkpointing for record $r").orDie *>
                                  checkpointer.checkpoint
                                    .tapError(e => ZIO(println(s"Checkpointing failed: ${e}")))
                                    .tap(_ =>
                                      ZIO(println(s"Checkpointing for shard ${r.shardId} done (${r.sequenceNumber}"))
                                    )
                              }
                        }
          } yield stream

        for {
          newShards            <- Queue.unbounded[Unit]
          requestShutdown      <- Promise.make[Nothing, Unit]
          firstRecordProcessed <- Promise.make[Nothing, Unit]

          // Act
          _        <- TestUtil
                 .produceRecords(streamName, 20000, 10, 10)
                 .fork
          consumer <- streamConsumer(
                        requestShutdown,
                        firstRecordProcessed,
                        newShards
                      ).runCollect
                        .tapError(e => zio.logging.log.error(s"Error in stream consumer,: ${e}"))
                        .fork
          _        <- firstRecordProcessed.await
          _         = println("Resharding")
          _        <- kinesis
                 .updateShardCount(
                   model.UpdateShardCountRequest(streamName, nrShards * 2, ScalingType.UNIFORM_SCALING)
                 )
                 .mapError(_.toThrowable)
          _        <- ZStream.fromQueue(newShards).take(nrShards * 3L).runDrain
          _         = println("All (new) shards seen")
          // The long timeout is related to LeaseCleanupConfig.completedLeaseCleanupIntervalMillis which currently cannot be configured in zio-kinesis
          _        <- ZIO.sleep(360.seconds)
          _        <- requestShutdown.succeed(())
          _        <- consumer.join
        } yield assertCompletes
      }
    } @@ TestAspect.timeout(5.minutes) @@ TestAspect.ifEnvSet("ENABLE_AWS")

  // TODO check the order of received records is correct

  override def spec =
    suite("DynamicConsumer")(
      testConsume1,
      testConsume2,
      testCheckpointAtShutdown,
      testShardEnd
    ).provideCustomLayer(env.orDie) @@ timeout(10.minutes)

  def delayStream[R, E, O](s: ZStream[R, E, O], delay: Duration) =
    ZStream.fromEffect(ZIO.sleep(delay)).flatMap(_ => s)

  def awaitRefPredicate[T](ref: Ref[T])(predicate: T => Boolean) =
    (for {
      p <- Promise.make[Nothing, Unit]
      _ <- ZIO
             .whenM(ref.get.map(predicate))(p.succeed(()))
             .repeat(Schedule.fixed(1.second))
             .fork
      _ <- p.await
    } yield ()).fork

}
