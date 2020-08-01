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
import zio.test.TestAspect._
import zio.test._
import zio.test.Assertion._

object ConsumeWithTest extends DefaultRunnableSpec {
  import TestUtil._

  private val nrRecords = 4

  private val loggingLayer: ZLayer[Any, Nothing, Logging] =
    Console.live ++ Clock.live >>> Logging.console(
      format = (_, logEntry) => logEntry,
      rootLoggerName = Some("default-logger")
    )

  private val env =
    (LocalStackServices.localHttpClient >>> LocalStackServices.kinesisAsyncClientLayer >>> (Client.live ++ AdminClient.live ++ LocalStackServices.dynamicConsumerLayer)) ++ Clock.live ++ Blocking.live ++ loggingLayer

  def testConsume1 =
    testM("consume records produced on all shards produced on the stream") {
      val streamName      = "zio-test-stream-" + UUID.randomUUID().toString
      val applicationName = "zio-test-" + UUID.randomUUID().toString

      for {
        refProcessed      <- Ref.make(Seq.empty[String])
        finishedConsuming <- Promise.make[Nothing, Unit]
        assert            <- createStream(streamName, 2).use { _ =>
                    val records =
                      (1 to nrRecords).map(i => ProducerRecord(s"key$i", s"msg$i"))
                    (for {
                      _                <- putStrLn("Putting records")
                      _                <- ZIO
                             .accessM[Client](
                               _.get
                                 .putRecords(
                                   streamName,
                                   Serde.asciiString,
                                   records
                                 )
                             )
                             .tapError(e => putStrLn(s"error1: $e").provideLayer(Console.live))
                             .retry(retryOnResourceNotFound)
                      _                <- putStrLn("Starting dynamic consumer")
                      consumerFiber    <- consumeWith[Any, String](
                                         streamName,
                                         applicationName = applicationName,
                                         deserializer = Serde.asciiString,
                                         isEnhancedFanOut = false,
                                         batchSize = 2
                                       ) {
                                         FakeRecordProcessor
                                           .process(
                                             refProcessed,
                                             finishedConsuming,
                                             expectedCountOrFailFunction = Right(nrRecords)
                                           )
                                       }.fork
                      _                <- finishedConsuming.await
                      _                <- consumerFiber.interrupt
                      processedRecords <- refProcessed.get
                    } yield assert(processedRecords.distinct.size)(equalTo(nrRecords)))
                  }
      } yield assert

    }

  override def spec =
    suite("ConsumeWithTest")(
      testConsume1
    ).provideCustomLayer(env.orDie) @@ timeout(60.seconds) @@ sequential

}
