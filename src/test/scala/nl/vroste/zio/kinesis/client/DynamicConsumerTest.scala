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
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._
import zio.{ Schedule, ZIO }

object DynamicConsumerTest extends DefaultRunnableSpec {
  private val retryOnResourceNotFound: Schedule[Clock, Throwable, ((Throwable, Int), Duration)] =
    Schedule.doWhile[Throwable] {
      case _: ResourceNotFoundException => true
      case _                            => false
    } &&
      Schedule.recurs(5) &&
      Schedule.exponential(2.second)

  private val createStream = (streamName: String, nrShards: Int) =>
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

  override def spec =
    suite("DynamicConsumer")(
      testM("consume records produced on all shards produced on the stream") {

        val streamName      = "zio-test-stream-" + UUID.randomUUID().toString
        val applicationName = "zio-test-" + UUID.randomUUID().toString

        (Client.build(LocalStackDynamicConsumer.kinesisAsyncClientBuilder) <* createStream(streamName, 2)).use {
          client =>
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
      },
      testM("support multiple parallel consumers on the same Kinesis stream") {

        val streamName      = "zio-test-stream-" + UUID.randomUUID().toString
        val applicationName = "zio-test-" + UUID.randomUUID().toString

        val nrRecords = 80

        def streamConsumer(label: String) = {
          val checkpointDivisor = 500

          def handler(shardId: String, r: DynamicConsumer.Record[String]) =
            for {
              id <- ZIO.fiberId
              _  <- putStrLn(s"Consumer $label on fiber $id got record $r on shard $shardId")
            } yield ()

          ZStream.fromEffect(putStrLn(s"Starting consumer $label")) *> LocalStackDynamicConsumer
            .shardedStream(
              streamName,
              applicationName = applicationName,
              deserializer = Serde.asciiString
            )
            .flatMapPar(Int.MaxValue) {
              case (shardId, shardStream) =>
                shardStream.zipWithIndex.tap {
                  case (r: DynamicConsumer.Record[String], sequenceNumberForShard: Long) =>
                    handler(shardId, r).as(r) <*
                      (putStrLn(
                        s"Checkpointing at offset ${sequenceNumberForShard} in consumer ${label}, shard ${shardId}"
                      ) *> r.checkpoint.catchSome {
                        case _: ShutdownException => // This will be throw when the shard lease has been stolen
                          ZIO.unit
                      }).when(sequenceNumberForShard % checkpointDivisor == checkpointDivisor - 1)
                        .tapError(_ => putStrLn(s"Failed to checkpoint in consumer ${label}, shard ${shardId}"))
                }.map(_._1)
                  .as((label, shardId))
                  .ensuring(putStrLn(s"Shard $shardId completed for consumer $label"))
            }
        }

        (Client.build(LocalStackDynamicConsumer.kinesisAsyncClientBuilder) <* createStream(streamName, 10)).use {
          client =>
            val records =
              (1 to nrRecords).map(i => ProducerRecord(s"key$i", s"msg$i"))
            for {
              _ <- putStrLn("Putting records")
              _ <- ZStream
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

              _ <- putStrLn("Starting dynamic consumers")

              records      <- (streamConsumer("1")
                             merge ZStream
                               .fromEffect(sleep(5.seconds))
                               .flatMap(_ => streamConsumer("2")))
                           .take(nrRecords * nrRecords.toLong)
                           .runCollect
              // Both consumers should have gotten some records
              usedConsumers = records.map(_._1).toSet
            } yield assert(usedConsumers)(equalTo(Set("1", "2")))
        }
      }
    ) @@ timeout(5.minute) @@ sequential

  def sleep(d: Duration) = ZIO.sleep(d).provideLayer(Clock.live)
}
