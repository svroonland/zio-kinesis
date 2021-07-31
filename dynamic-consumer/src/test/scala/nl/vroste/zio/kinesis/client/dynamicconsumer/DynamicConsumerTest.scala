package nl.vroste.zio.kinesis.client.dynamicconsumer

import io.github.vigoo.zioaws.cloudwatch.CloudWatch
import io.github.vigoo.zioaws.dynamodb.DynamoDb
import io.github.vigoo.zioaws.dynamodb.model.DeleteTableRequest
import io.github.vigoo.zioaws.{ dynamodb, kinesis }
import io.github.vigoo.zioaws.kinesis.model.ScalingType
import io.github.vigoo.zioaws.kinesis.{ model, Kinesis }
import nl.vroste.zio.kinesis.client
import nl.vroste.zio.kinesis.client.localstack.LocalStackServices
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.{ ProducerRecord, TestUtil }
import software.amazon.kinesis.exceptions.ShutdownException
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.{ putStrLn, Console }
import zio.duration.{ durationInt, Duration }
import zio.logging.{ LogLevel, Logging }
import zio.stream.{ ZStream, ZTransducer }
import zio.test.Assertion._
import zio.test.TestAspect.{ sequential, timeout }
import zio.test._

import java.util.UUID

object DynamicConsumerTest extends DefaultRunnableSpec {
  import TestUtil._

  val loggingLayer: ZLayer[Any, Nothing, Logging] =
    (Console.live ++ Clock.live) >>> Logging.console(LogLevel.Trace) >>> Logging.withRootLoggerName(getClass.getName)

  val useAws = Runtime.default.unsafeRun(system.envOrElse("ENABLE_AWS", "0")).toInt == 1

  private val env: ZLayer[
    Any,
    Throwable,
    CloudWatch with Kinesis with DynamoDb with DynamicConsumer with Clock with Blocking with Logging
  ] = (if (useAws) client.defaultAwsLayer else LocalStackServices.localStackAwsLayer()) >+> loggingLayer >+>
    (DynamicConsumer.live ++ Clock.live ++ Blocking.live)

  def testConsume1 =
    testM("consume records produced on all shards produced on the stream") {
      withRandomStreamEnv(2) { (streamName, applicationName) =>
        (for {
          _ <- putStrLn("Putting records").orDie
          _ <- TestUtil
                 .putRecords(
                   streamName,
                   Serde.asciiString,
                   Seq(ProducerRecord("key1", "msg1"), ProducerRecord("key2", "msg2"))
                 )
                 .tapError(e => putStrLn(s"error1: $e").provideLayer(Console.live).orDie)
                 .retry(retryOnResourceNotFound)

          _ <- putStrLn("Starting dynamic consumer").orDie
          _ <- (for {
                   service <- ZIO.service[DynamicConsumer.Service]
                   _       <- service
                          .shardedStream(
                            streamName,
                            applicationName = applicationName,
                            deserializer = Serde.asciiString,
                            isEnhancedFanOut = false
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
                 } yield ())

        } yield assertCompletes)
      // TODO this assertion doesn't do what the test says
      }
    }

  def testConsume2 =
    testM("support multiple parallel consumers on the same Kinesis stream") {
      withRandomStreamEnv(10) { (streamName, applicationName) =>
        val nrRecords = 80

        def streamConsumer(
          workerIdentifier: String,
          activeConsumers: Ref[Set[String]]
        ): ZStream[Console with Blocking with DynamicConsumer with Clock, Throwable, (String, String)] = {
          val checkpointDivisor = 1

          def handler(shardId: String, r: DynamicConsumer.Record[String]) =
            for {
              id <- ZIO.fiberId
              _  <- putStrLn(s"Consumer $workerIdentifier on fiber $id got record $r on shard $shardId").orDie
              // Simulate some effectful processing
              _  <- ZIO.sleep(50.millis)
            } yield ()

          (for {
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
                              isEnhancedFanOut = false
                            )
                            .flatMapPar(Int.MaxValue) {
                              case (shardId, shardStream, checkpointer @ _) =>
                                shardStream.zipWithIndex.tap {
                                  case (r: DynamicConsumer.Record[String], sequenceNumberForShard: Long) =>
                                    checkpointer.stageOnSuccess(handler(shardId, r))(r).as(r) <*
                                      (putStrLn(
                                        s"Checkpointing at offset ${sequenceNumberForShard} in consumer ${workerIdentifier}, shard ${shardId}"
                                      ).orDie *> checkpointer.checkpoint)
                                        .when(sequenceNumberForShard % checkpointDivisor == checkpointDivisor - 1)
                                        .tapError(_ =>
                                          putStrLn(
                                            s"Failed to checkpoint in consumer ${workerIdentifier}, shard ${shardId}"
                                          ).orDie
                                        )
                                }.as((workerIdentifier, shardId))
                                  // Background and a bit delayed so we get a chance to actually emit some records
                                  .tap(_ => activeConsumers.update(_ + workerIdentifier).delay(1.second).fork)
                                  .ensuring(putStrLn(s"Shard $shardId completed for consumer $workerIdentifier").orDie)
                                  .catchSome {
                                    case _: ShutdownException => // This will be thrown when the shard lease has been stolen
                                      // Abort the stream when we no longer have the lease
                                      ZStream.empty
                                  }
                            }
                        )

          } yield stream)
        }

        val records =
          (1 to nrRecords).map(i => ProducerRecord(s"key$i", s"msg$i"))
        for {
          _                     <- putStrLn("Putting records").orDie
          producer              <- ZStream
                        .fromIterable(1 to nrRecords)
                        .schedule(Schedule.spaced(250.millis))
                        .mapM { _ =>
                          TestUtil
                            .putRecords(streamName, Serde.asciiString, records)
                            .tapError(e => putStrLn(s"error2: $e").provideLayer(Console.live).orDie)
                            .retry(retryOnResourceNotFound)
                        }
                        .runDrain
                        .fork

          _                     <- putStrLn("Starting dynamic consumers").orDie
          activeConsumers       <- Ref.make[Set[String]](Set.empty)
          allConsumersGotAShard <- awaitRefPredicate(activeConsumers)(_ == Set("1", "2"))
          _                     <- (streamConsumer("1", activeConsumers)
                   merge delayStream(streamConsumer("2", activeConsumers), 5.seconds))
                 .interruptWhen(allConsumersGotAShard.join)
                 .runCollect
          _                     <- producer.interrupt
        } yield assertCompletes
      }
    }

  def testCheckpointAtShutdown =
    testM("checkpoint for the last processed record at stream shutdown") {
      withRandomStreamEnv(2) { (streamName, applicationName) =>
        val batchSize = 100
        val nrBatches = 8

        def streamConsumer(
          interrupted: Promise[Nothing, Unit],
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
                          isEnhancedFanOut = false,
                          requestShutdown = interrupted.await.tap(_ => UIO(println("Interrupting shardedStream")))
                        )
                        .flatMapPar(Int.MaxValue) {
                          case (shardId, shardStream, checkpointer @ _) =>
                            shardStream
                              .tap(record => lastProcessedRecords.update(_ + (shardId -> record.sequenceNumber)))
                              .tap(checkpointer.stage)
                              .tap(record =>
                                ZIO.when(record.partitionKey == "key500")(
                                  putStrLn(s"Interrupting for partition key ${record.partitionKey}").orDie
                                    *> interrupted.succeed(())
                                )
                              )
                              // .tap(r => putStrLn(s"Shard ${shardId} got record ${r.data}").orDie)
                              // It's important that the checkpointing is always done before flattening the stream, otherwise
                              // we cannot guarantee that the KCL has not yet shutdown the record processor and taken away the lease
                              .aggregateAsyncWithin(
                                ZTransducer.last, // TODO we need to make sure that in our test this thing has some records buffered after shutdown request
                                Schedule.fixed(1.seconds)
                              )
                              .mapConcat(_.toList)
                              .tap { r =>
                                putStrLn(s"Shard ${r.shardId}: checkpointing for record $r $interrupted").orDie *>
                                  checkpointer.checkpoint
                                    .tapError(e => ZIO(println(s"Checkpointing failed: ${e}")))
                                    .tap(_ => lastCheckpointedRecords.update(_ + (shardId -> r.sequenceNumber)))
                                    .tap(_ => ZIO(println(s"Checkpointing for shard ${r.shardId} done")))
                              }
                        }
          } yield stream)

        for {
          _                         <- ZStream
                 .fromIterable(1 to nrBatches)
                 .schedule(Schedule.spaced(250.millis))
                 .mapM { batchIndex =>
                   TestUtil
                     .putRecords(
                       streamName,
                       Serde.asciiString,
                       recordsForBatch(batchIndex, batchSize)
                         .map(i => ProducerRecord(s"key$i", s"msg$i"))
                     )
                     .tapError(e => putStrLn(s"error3: $e").orDie)
                     .retry(retryOnResourceNotFound)
                 }
                 .runDrain
                 .tap(_ => ZIO(println("PRODUCING RECORDS DONE")))

          interrupted               <- Promise
                           .make[Nothing, Unit]
          lastProcessedRecords      <- Ref.make[Map[String, String]](Map.empty) // Shard -> Sequence Nr
          lastCheckpointedRecords   <- Ref.make[Map[String, String]](Map.empty) // Shard -> Sequence Nr
          consumer                  <- streamConsumer(interrupted, lastProcessedRecords, lastCheckpointedRecords).runCollect.fork
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
                          isEnhancedFanOut = false,
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
          // The long timeout is related to LeaseCleanupConfig.completedLeaseCleanupIntervalMillis which I don't see how to alter
          _        <- ZIO.sleep(20.seconds)
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
    ).provideCustomLayerShared(env.orDie) @@ timeout(10.minutes) @@ sequential

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
