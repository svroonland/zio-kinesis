package nl.vroste.zio.kinesis.client

import java.util.UUID

import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.DynamicConsumer.consumeWith
import nl.vroste.zio.kinesis.client.serde.Serde
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console._
import zio.duration._
import zio.logging.Logging
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._

object ConsumeWithReshardTest extends DefaultRunnableSpec {
  import TestUtil._

  private val loggingLayer: ZLayer[Any, Nothing, Logging] =
    Console.live ++ Clock.live >>> Logging.console(
      format = (_, logEntry) => logEntry,
      rootLoggerName = Some("default-logger")
    )

  private val env =
    (LocalStackServices.localHttpClient >>> LocalStackServices.kinesisAsyncClientLayer >>> (Client.live ++ AdminClient.live ++ LocalStackServices.dynamicConsumerLayer)) ++ Clock.live ++ Blocking.live ++ loggingLayer

  def testConsume2 =
    testM(
      "consumeWith should, after a re-shard consume records produced on all shards"
    ) {
      val streamName        = "zio-test-stream-" + UUID.randomUUID().toString
      val applicationName   = "zio-test-" + UUID.randomUUID().toString
      val nrRecordsPerBatch = 100
      val nrBatches         = 100

      for {
        refProcessed      <- Ref.make(Seq.empty[String])
        finishedConsuming <- Promise.make[Nothing, Unit]
        assert            <- createStream(streamName, 2).use { _ =>
                    (for {
                      _                <- putStrLn("Putting records")
                      _                <- ZStream
                             .fromIterable(1 to nrBatches)
                             .schedule(Schedule.spaced(250.millis))
                             .mapM { batchIndex =>
                               ZIO
                                 .accessM[Client](
                                   _.get
                                     .putRecords(
                                       streamName,
                                       Serde.asciiString,
                                       recordsForBatch(batchIndex, nrRecordsPerBatch)
                                         .map(i => ProducerRecord(s"key$i", s"msg$i"))
                                     )
                                 )
                             }
                             .runDrain
                             .tapError(e => putStrLn(s"error1: $e").provideLayer(Console.live))
                             .retry(retryOnResourceNotFound)
                             .fork
                      _                <- putStrLn("Starting dynamic consumer")
                      consumerFiber    <- consumeWith[Any, Logging, String](
                                         streamName,
                                         applicationName = applicationName,
                                         deserializer = Serde.asciiString,
                                         isEnhancedFanOut = false,
                                         checkpointBatchSize = nrRecordsPerBatch
                                       ) {
                                         FakeRecordProcessor
                                           .make(
                                             refProcessed,
                                             finishedConsuming,
                                             expectedCount = nrRecordsPerBatch * nrBatches
                                           )
                                       }.fork
                      _                <- ZIO
                             .accessM[AdminClient](
                               _.get.mergeShards(streamName, "shardId-000000000000", "shardId-000000000001")
                             )
                             .delay(1.seconds)
                             .fork
                      _                <- finishedConsuming.await
                      _                <- consumerFiber.interrupt
                      processedRecords <- refProcessed.get
                    } yield assert(processedRecords.distinct.size)(equalTo(nrRecordsPerBatch * nrBatches)))
                  }
      } yield assert

    }

  override def spec =
    suite("ConsumeWithTest2")(
      testConsume2
    ).provideCustomLayer(env.orDie) @@ timeout(2.minutes) @@ sequential

}
