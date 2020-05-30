package nl.vroste.zio.kinesis.client

import java.util.UUID

import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.serde.Serde
import software.amazon.awssdk.services.kinesis.model.{ ResourceInUseException, ResourceNotFoundException }
import software.amazon.kinesis.exceptions.ShutdownException
import zio.clock.Clock
import zio.console._
import zio.duration._
import zio.stream.ZStream
import zio.test.TestAspect._
import zio.test._
import zio.{ Promise, Ref, Schedule, ZIO, ZManaged }

object DynamicConsumerTest extends DefaultRunnableSpec {
  private val retryOnResourceNotFound: Schedule[Clock, Throwable, ((Throwable, Int), Duration)] =
    Schedule.doWhile[Throwable] {
      case _: ResourceNotFoundException => true
      case _                            => false
    } &&
      Schedule.recurs(5) &&
      Schedule.exponential(2.second)

  private def createStream(streamName: String, nrShards: Int): ZManaged[Console, Throwable, Unit] =
    for {
      adminClient <- AdminClient.build(LocalStackDynamicConsumer.kinesisAsyncClientBuilder)
      _           <- adminClient
             .createStream(streamName, nrShards)
             .catchSome {
               case _: ResourceInUseException =>
                 putStrLn("Stream already exists")
             }
             .toManaged { _ =>
               adminClient
                 .deleteStream(streamName, enforceConsumerDeletion = true)
                 .catchSome {
                   case _: ResourceNotFoundException => ZIO.unit
                 }
                 .orDie
             }
    } yield ()

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
                 .flatMapPar(Int.MaxValue)(_._2)
                 .take(2)
                 .tap(r => putStrLn(s"Got record $r") *> r.checkpoint.retry(Schedule.exponential(100.millis)))
                 .runCollect
        } yield assertCompletes
      }
    }

  def testConsume2 =
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
                case (shardId, shardStream) =>
                  shardStream.zipWithIndex.tap {
                    case (r: DynamicConsumer.Record[String], sequenceNumberForShard: Long) =>
                      handler(shardId, r).as(r) <*
                        (putStrLn(
                          s"Checkpointing at offset ${sequenceNumberForShard} in consumer ${workerIdentifier}, shard ${shardId}"
                        ) *> r.checkpoint)
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

  override def spec =
    suite("DynamicConsumer")(
      testConsume1,
      testConsume2
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
