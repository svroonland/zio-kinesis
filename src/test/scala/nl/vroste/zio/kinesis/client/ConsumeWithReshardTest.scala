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
    (HttpClient
      .make() >>> sdkClients >>> (Client.live ++ AdminClient.live ++ DynamicConsumer.live)) ++ Clock.live ++ Blocking.live ++ loggingLayer

  def testConsume2 =
    testM(
      "consumeWith should, after a re-shard consume records produced on all shards"
    ) {
      val streamName      = "mercury-invoice-test-reshard-dev"
      val applicationName = "mercury-invoice-test-reshard-dev"

      val nrRecordsPerBatch = 100
      val nrBatches         = 30

      val nrShards             = 10
      val nrReshardShards: Int = nrShards / 2

      for {
        refProcessed      <- Ref.make(Seq.empty[String])
        finishedConsuming <- Promise.make[Nothing, Unit]
        assert            <- createStream(streamName, nrShards).use { _ =>
                    (for {
                      _                <- putStrLn("Putting records")
                      _                <- ZStream
                             .fromIterable(1 to nrBatches)
                             .schedule(Schedule.spaced(500.millis))
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
                             .tapError(e => putStrLn(s"error: $e").provideLayer(Console.live))
                             .retry(retryOnResourceNotFound)
                             .fork
                      _                <- putStrLn("Starting dynamic consumer")
                      consumerFiber    <- consumeWith[Any, Logging, String](
                                         streamName,
                                         applicationName = applicationName,
                                         deserializer = Serde.asciiString,
                                         isEnhancedFanOut = false,
                                         checkpointBatchSize = nrRecordsPerBatch.toLong,
                                         checkpointDuration = 10.second
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
                               _.get.updateShardCount(streamName, nrReshardShards)
                             )
                             .delay(16.seconds)
                             .fork
                      _                <- finishedConsuming.await
                      _                <- consumerFiber.interrupt
                      processedRecords <- refProcessed.get
                    } yield assert(processedRecords.distinct.size)(equalTo(nrRecordsPerBatch * nrBatches)))
                  }
      } yield assert

    }

  override def spec =
    suite("ConsumeWithReshardTest")(
      testConsume2
    ).provideCustomLayer(env.orDie) @@ timeout(2.minutes) @@ sequential

}
