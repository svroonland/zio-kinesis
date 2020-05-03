package nl.vroste.zio.kinesis.client

import java.util.UUID

import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.serde.Serde
import software.amazon.awssdk.services.kinesis.model.{ ResourceInUseException, ResourceNotFoundException }
import zio.blocking.Blocking
import zio.clock.{ sleep, Clock }
import zio.duration._
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._
import zio.{ Fiber, Schedule, ZIO }

object DynamicConsumerTest2 extends DefaultRunnableSpec {
  private val retryOnResourceNotFound = Schedule.doWhile[Throwable] {
    case _: ResourceNotFoundException => true
    case _                            => false
  } &&
    Schedule.recurs(5) &&
    Schedule.exponential(2.second)

  val createStream = (streamName: String, nrShards: Int) =>
    for {
      adminClient <- AdminClient.build(LocalStackDynamicConsumer.kinesisAsyncClientBuilder)
      _ <- adminClient
            .createStream(streamName, nrShards)
            .catchSome {
              case _: ResourceInUseException =>
                println("Stream already exists")
                ZIO.unit
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

  def spec =
    suite("DynamicConsumer")(
      testM("consume records produced on all shards produced on the stream") {

        val streamName      = "zio-test-stream-" + UUID.randomUUID().toString
        val applicationName = "zio-test-" + UUID.randomUUID().toString

        (Client.build(LocalStackDynamicConsumer.kinesisAsyncClientBuilder) <* createStream(streamName, 2)).use {
          client =>
            println("Putting records")
            for {
              _ <- client
                    .putRecords(
                      streamName,
                      Serde.asciiString,
                      Seq(ProducerRecord("key1", "msg1"), ProducerRecord("key2", "msg2"))
                    )
                    .retry(retryOnResourceNotFound)
                    .provideLayer(Clock.live)

              _ = println("Starting dynamic consumer")
              _ <- LocalStackDynamicConsumer
                    .shardedStream(
                      streamName,
                      applicationName = applicationName,
                      deserializer = Serde.asciiString
                    )
                    .flatMapPar(Int.MaxValue)(_._2.flattenChunks)
                    .take(2)
                    .tap(r => ZIO(println(s"Got record ${r}")) *> r.checkpoint.retry(Schedule.exponential(100.millis)))
                    .runCollect
            } yield assertCompletes
        }
      } @@ ignore,
      testM("support multiple parallel streams") {

        val streamName      = "zio-test-stream-" + UUID.randomUUID().toString
        val applicationName = "zio-test-" + UUID.randomUUID().toString

//        val nrRecords = 40

        def streamConsumer(label: String): ZStream[Clock with Blocking, Throwable, (String, String)] =
          LocalStackDynamicConsumer
            .shardedStream(
              streamName,
              applicationName = applicationName,
              deserializer = Serde.asciiString
            )
            .flatMapPar(Int.MaxValue) {
              case (shardID, shardStream) =>
                shardStream.flattenChunks.tap { r =>
                  ZIO.fiberId andThen
                    ZIO.fromFunction((id: Fiber.Id) =>
                      println(s"Consumer ${label} on fiber ${id} got record ${r} on shard ${shardID}")
                    )
                }.tap(_.checkpoint.retry(Schedule.exponential(100.millis)))
                  .map(_ => (label, shardID))
                  .ensuring(ZIO(println(s"Shard ${shardID} completed for consumer ${label}")).orDie)
            }

//        val x: ZStream[Clock with Blocking, Throwable, (String, String)] = streamConsumer("1") merge streamConsumer("1")

        val maxRecords   = 800
        val maxBatchSize = 200

        def putRecordsEmitter(
          streamName: String,
          batchSize: Int,
          max: Int,
          client: Client
        ): ZStream[Clock, Throwable, Int] =
          ZStream.unfoldM(1) { i =>
            if (i < max) {
              val recordsBatch = (i until i + batchSize).map(i => ProducerRecord(s"key${i}", s"msg${i}"))
              val putRecordsM = client
                .putRecords(
                  streamName,
                  Serde.asciiString,
                  recordsBatch
                )
                .retry(retryOnResourceNotFound)
              for {
                _ <- sleep(250.milliseconds)
                _ <- putRecordsM
              } yield Some((i, i + batchSize))
            } else {
              ZIO.succeed(None)
            }
          }

        (Client.build(LocalStackDynamicConsumer.kinesisAsyncClientBuilder) <* createStream(streamName, 10)).use {
          client =>
            println("Putting records")
//            val records =
//              (1 to nrRecords).map(i => ProducerRecord(s"key${i}", s"msg${i}"))
            for {
//              _ <- ZStream
//                    .fromIterable((1 to nrRecords))
//                    .schedule(Schedule.spaced(250.millis))
//                    .mapM { _ =>
//                      client
//                        .putRecords(streamName, Serde.asciiString, records)
//                        .tapError(e => ZIO(println(e)))
//                        .retry(retryOnResourceNotFound)
//                    }
//                    .provideLayer(Clock.live)
//                    .runDrain
//                    .fork
              _ <- putRecordsEmitter(
                    streamName,
                    batchSize = maxBatchSize,
                    max = maxRecords,
                    client
                  ).provideLayer(Clock.live).runDrain
              _ = println("Starting dynamic consumer")

              records <- (streamConsumer("1")
                          merge ZStream
                            .fromEffect(ZIO.sleep(5.seconds).provideLayer(Clock.live))
                            .flatMap(_ => streamConsumer("2")))
                          .take(maxRecords.toLong - 100L)
                          .runCollect
              _ = records.foreach(println)
              // Both consumers should have gotten some records
            } yield assert(records.map(_._1).toSet)(equalTo(Set("1", "2")))
        }
      }
    ) @@ timeout(3.minute) @@ sequential
}
