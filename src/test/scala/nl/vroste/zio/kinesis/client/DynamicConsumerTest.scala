package nl.vroste.zio.kinesis.client

import java.util.UUID

import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.serde.Serde
import software.amazon.kinesis.exceptions.ShutdownException
import zio._
import zio.clock.Clock
import zio.console._
import zio.duration._
import zio.stream.{ ZStream, ZTransducer }
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._

object DynamicConsumerTest extends DefaultRunnableSpec {
  import TestUtil._

  def testConsume1 =
    testM("consume records produced on all shards produced on the stream") {
      val streamName      = "zio-test-stream-" + UUID.randomUUID().toString
      val applicationName = "zio-test-" + UUID.randomUUID().toString

      (Client.build(LocalStackDynamicConsumer.kinesisAsyncClientBuilder) <* createStream(streamName, 2)).use { client =>
        for {
          _ <- putStrLn("Putting records")
          _ <- client
                 .putRecords(
                   streamName,
                   Serde.asciiString,
                   Seq(ProducerRecord("key1", "msg1"), ProducerRecord("key2", "msg2"))
                 )
                 .retry(retryOnResourceNotFound)
                 .provideLayer(Clock.live)

          _ <- putStrLn("Starting dynamic consumer")
          _ <- LocalStackDynamicConsumer
                 .shardedStream(
                   streamName,
                   applicationName = applicationName,
                   deserializer = Serde.asciiString
                 )
                 .flatMapPar(Int.MaxValue) {
                   case (shardId @ _, shardStream, checkpointer) =>
                     shardStream.tap(r =>
                       putStrLn(s"Got record $r") *> checkpointer
                         .checkpointNow(r)
                         .retry(Schedule.exponential(100.millis))
                     )
                 }
                 .take(2)
                 .runCollect
        } yield assertCompletes // TODO this assertion doesn't do what the test says
      }
    }

  def testConsume2             =
    testM("support multiple parallel consumers on the same Kinesis stream") {

      val streamName      = "zio-test-stream-" + UUID.randomUUID().toString
      val applicationName = "zio-test-" + UUID.randomUUID().toString

      val nrRecords = 80

      def streamConsumer(workerIdentifier: String, activeConsumers: Ref[Set[String]]) = {
        val checkpointDivisor = 1

        def handler(shardId: String, r: DynamicConsumer.Record[String]) =
          for {
            id <- ZIO.fiberId
            _  <- putStrLn(s"Consumer $workerIdentifier on fiber $id got record $r on shard $shardId")
            // Simulate some effectful processing
            _  <- sleep(50.millis)
          } yield ()

        ZStream
          .fromEffect(putStrLn(s"Starting consumer $workerIdentifier"))
          .flatMap(_ =>
            LocalStackDynamicConsumer
              .shardedStream(
                streamName,
                applicationName = applicationName,
                deserializer = Serde.asciiString,
                workerIdentifier = applicationName + "-" + workerIdentifier
              )
              .flatMapPar(Int.MaxValue) {
                case (shardId, shardStream, checkpointer @ _) =>
                  shardStream.zipWithIndex.tap {
                    case (r: DynamicConsumer.Record[String], sequenceNumberForShard: Long) =>
                      checkpointer.stageOnSuccess(handler(shardId, r))(r).as(r) <*
                        (putStrLn(
                          s"Checkpointing at offset ${sequenceNumberForShard} in consumer ${workerIdentifier}, shard ${shardId}"
                        ) *> checkpointer.checkpoint)
                          .when(sequenceNumberForShard % checkpointDivisor == checkpointDivisor - 1)
                          .tapError(_ =>
                            putStrLn(s"Failed to checkpoint in consumer ${workerIdentifier}, shard ${shardId}")
                          )
                  }.as((workerIdentifier, shardId))
                    // Background and a bit delayed so we get a chance to actually emit some records
                    .tap(_ => (sleep(1.second) *> activeConsumers.update(_ + workerIdentifier)).fork)
                    .ensuring(putStrLn(s"Shard $shardId completed for consumer $workerIdentifier"))
                    .catchSome {
                      case _: ShutdownException => // This will be thrown when the shard lease has been stolen
                        // Abort the stream when we no longer have the lease
                        ZStream.empty
                    }
              }
          )
      }

      (Client.build(LocalStackDynamicConsumer.kinesisAsyncClientBuilder) <* createStream(streamName, 10)).use {
        client =>
          val records =
            (1 to nrRecords).map(i => ProducerRecord(s"key$i", s"msg$i"))
          for {
            _                     <- putStrLn("Putting records")
            _                     <- ZStream
                   .fromIterable(1 to nrRecords)
                   .schedule(Schedule.spaced(250.millis))
                   .mapM { _ =>
                     client
                       .putRecords(streamName, Serde.asciiString, records)
                       .tapError(e => putStrLn(s"error: $e").provideLayer(Console.live))
                       .retry(retryOnResourceNotFound)
                   }
                   .provideSomeLayer(Clock.live)
                   .runDrain
                   .fork

            _                     <- putStrLn("Starting dynamic consumers")
            activeConsumers       <- Ref.make[Set[String]](Set.empty)
            allConsumersGotAShard <- awaitRefPredicate(activeConsumers)(_ == Set("1", "2"))
            _                     <- (streamConsumer("1", activeConsumers)
                     merge delayStream(streamConsumer("2", activeConsumers), 5.seconds))
                   .interruptWhen(allConsumersGotAShard.join)
                   .runCollect
          } yield assertCompletes
      }
    }
  def testCheckpointAtShutdown =
    testM("checkpoint for the last processed record at stream shutdown") {
      val streamName      = "zio-test-stream-" + UUID.randomUUID().toString
      val applicationName = "zio-test-" + UUID.randomUUID().toString

      val batchSize = 100
      val nrBatches = 100
      val records   =
        (1 to batchSize).map(i => ProducerRecord(s"key$i", s"msg$i"))

      (Client.build(LocalStackDynamicConsumer.kinesisAsyncClientBuilder) <* createStream(streamName, 2)).use { client =>
        for {
          producing                 <- ZStream
                         .fromIterable(1 to nrBatches)
                         .schedule(Schedule.spaced(250.millis))
                         .mapM { _ =>
                           client
                             .putRecords(streamName, Serde.asciiString, records)
                             //                             .tap(_ => putStrLn("Put records on stream"))
                             .tapError(e => putStrLn(s"error: $e").provideLayer(Console.live))
                             .retry(retryOnResourceNotFound)
                         }
                         .runDrain
                         .tap(_ => ZIO(println("PRODUCING RECORDS DONE")))
                         .forkAs("RecordProducing")

          interrupted               <- Promise
                           .make[Nothing, Unit]
                           .tap(p => (putStrLn("INTERRUPTING") *> p.succeed(())).delay(19.seconds + 333.millis).fork)
          lastProcessedRecords      <- Ref.make[Map[String, String]](Map.empty) // Shard -> Sequence Nr
          lastCheckpointedRecords   <- Ref.make[Map[String, String]](Map.empty) // Shard -> Sequence Nr
          _                         <- LocalStackDynamicConsumer
                 .shardedStream(
                   streamName,
                   applicationName = applicationName,
                   deserializer = Serde.asciiString,
                   requestShutdown = interrupted.await.tap(_ => UIO(println("Interrupting shardedStream")))
                 )
                 .flatMapPar(Int.MaxValue) {
                   case (shardId, shardStream, checkpointer @ _) =>
                     shardStream
                       .tap(record => lastProcessedRecords.update(_ + (shardId -> record.sequenceNumber)))
                       .tap(checkpointer.stage)
                       // .tap(r => putStrLn(s"Shard ${shardId} got record ${r.data}"))
                       // It's important that the checkpointing is always done before flattening the stream, otherwise
                       // we cannot guarantee that the KCL has not yet shutdown the record processor and taken away the lease
                       .aggregateAsyncWithin(
                         ZTransducer.last, // TODO we need to make sure that in our test this thing has some records buffered after shutdown request
                         Schedule.fixed(1.seconds)
                       )
                       .mapConcat(_.toList)
                       .tap { r =>
                         putStrLn(s"Shard ${r.shardId}: checkpointing for record $r") *>
                           checkpointer.checkpoint
                             .tapError(e => ZIO(println(s"Checkpointing failed: ${e}")))
                             .tap(_ => lastCheckpointedRecords.update(_ + (shardId -> r.sequenceNumber)))
                             .tap(_ => ZIO(println(s"Checkpointing for shard ${r.shardId} done")))
                       }
                 }
                 .runCollect
          _                         <- producing.interrupt
          (processed, checkpointed) <- (lastProcessedRecords.get zip lastCheckpointedRecords.get)
        } yield assert(processed)(equalTo(checkpointed))
      }.provideCustomLayer(Clock.live)
    } @@ TestAspect.timeout(40.seconds)

  // TODO check the order of received records is correct

  override def spec =
    suite("DynamicConsumer")(
      testConsume1,
      testConsume2,
      testCheckpointAtShutdown @@ ignore
    ) @@ timeout(5.minute) @@ sequential

  def sleep(d: Duration) = ZIO.sleep(d).provideLayer(Clock.live)

  def delayStream[R, E, O](s: ZStream[R, E, O], delay: Duration) =
    ZStream.fromEffect(sleep(delay)).flatMap(_ => s)

  def awaitRefPredicate[T](ref: Ref[T])(predicate: T => Boolean) =
    (for {
      p <- Promise.make[Nothing, Unit]
      _ <- ZIO
             .whenM(ref.get.map(predicate))(p.succeed(()))
             .repeat(Schedule.fixed(1.second))
             .provideLayer(Clock.live)
             .fork
      _ <- p.await
    } yield ()).fork

}
