package nl.vroste.zio.kinesis.client

import java.util.UUID

import io.github.vigoo.zioaws.cloudwatch.CloudWatch
import io.github.vigoo.zioaws.dynamodb.DynamoDb
import io.github.vigoo.zioaws.kinesis.Kinesis
import nl.vroste.zio.kinesis.client.DynamicConsumer.consumeWith
import nl.vroste.zio.kinesis.client.localstack.LocalStackServices
import nl.vroste.zio.kinesis.client.serde.Serde
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console._
import zio.duration._
import zio.logging.Logging
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._

object ConsumeWithTest extends DefaultRunnableSpec {
  import TestUtil._

  val loggingLayer: ZLayer[Console with Clock, Nothing, Logging] =
    Logging.console() >>> Logging.withRootLoggerName(getClass.getName)

<<<<<<< HEAD
  private val env: ZLayer[
    Any,
    Throwable,
    Console with Clock with Logging with Blocking with Kinesis with CloudWatch with DynamoDb with Has[
      DynamicConsumer.Service
    ]
  ] =
    (Console.live ++ Clock.live ++ Blocking.live) >+> loggingLayer >+> LocalStackServices.env >+> DynamicConsumer.live
=======
  private val env
    : ZLayer[Any, Throwable, Console with Clock with Logging with Blocking with Has[CloudWatchAsyncClient] with Has[
      KinesisAsyncClient
    ] with Has[DynamoDbAsyncClient] with Has[DynamicConsumer.Service] with Client with AdminClient] =
    (Console.live ++ Clock.live ++ Blocking.live) >+> loggingLayer >+> LocalStackServices
      .localStackAwsLayer() >+> (DynamicConsumer.live ++ Client.live ++ AdminClient.live)
>>>>>>> origin/master

  def testConsume1 =
    testM("consumeWith should consume records produced on all shards") {
      val streamName      = "zio-test-stream-" + UUID.randomUUID().toString
      val applicationName = "zio-test-" + UUID.randomUUID().toString
      val nrRecords       = 4

      for {
        refProcessed      <- Ref.make(Seq.empty[String])
        finishedConsuming <- Promise.make[Nothing, Unit]
        assert            <- createStream(streamName, 2).use { _ =>
                    val records =
                      (1 to nrRecords).map(i => ProducerRecord(s"key$i", s"msg$i"))
                    (for {
                      _                <- putStrLn("Putting records")
                      _                <- putRecords(streamName, Serde.asciiString, records)
                             .tapError(e => putStrLn(s"error1: $e").provideLayer(Console.live))
                             .retry(retryOnResourceNotFound)
                      _                <- putStrLn("Starting dynamic consumer")
                      consumerFiber    <- consumeWith[Any, Logging, String](
                                         streamName,
                                         applicationName = applicationName,
                                         deserializer = Serde.asciiString,
                                         isEnhancedFanOut = false,
                                         checkpointBatchSize = 2
                                       ) {
                                         FakeRecordProcessor
                                           .make(
                                             refProcessed,
                                             finishedConsuming,
                                             expectedCount = nrRecords
                                           )
                                       }.fork
                      _                <- finishedConsuming.await
                      _                <- consumerFiber.interrupt
                      processedRecords <- refProcessed.get
                    } yield assert(processedRecords.distinct.size)(equalTo(nrRecords)))
                  }
      } yield assert

    }

  def testConsume2 =
    testM(
      "consumeWith should, after a restart due to a record processing error, consume records produced on all shards"
    ) {
      val streamName      = "zio-test-stream-" + UUID.randomUUID().toString
      val applicationName = "zio-test-" + UUID.randomUUID().toString
      val nrRecords       = 50
      val batchSize       = 10L

      for {
        refProcessed      <- Ref.make(Seq.empty[String])
        finishedConsuming <- Promise.make[Nothing, Unit]
        assert            <- createStream(streamName, 2).use { _ =>
                    val records =
                      (1 to nrRecords).map(i => ProducerRecord(s"key$i", s"msg$i"))
                    (for {
                      _                <- putStrLn("Putting records")
                      _                <- putRecords(streamName, Serde.asciiString, records)
                             .tapError(e => putStrLn(s"error1: $e").provideLayer(Console.live))
                             .retry(retryOnResourceNotFound)
                      _                <- putStrLn("Starting dynamic consumer - about to fail")
                      _                <- consumeWith[Any, Logging, String](
                             streamName,
                             applicationName = applicationName,
                             deserializer = Serde.asciiString,
                             isEnhancedFanOut = false,
                             checkpointBatchSize = batchSize
                           ) {
                             FakeRecordProcessor
                               .makeFailing(
                                 refProcessed,
                                 finishedConsuming,
                                 failFunction = _ == "msg31"
                               )
                           }.ignore
                      _                <- putStrLn("Starting dynamic consumer - about to succeed")
                      consumerFiber    <- consumeWith[Any, Logging, String](
                                         streamName,
                                         applicationName = applicationName,
                                         deserializer = Serde.asciiString,
                                         isEnhancedFanOut = false,
                                         checkpointBatchSize = batchSize
                                       ) {
                                         FakeRecordProcessor
                                           .make(
                                             refProcessed,
                                             finishedConsuming,
                                             expectedCount = nrRecords
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
      testConsume1,
      testConsume2
    ).provideLayer(env.orDie) @@ timeout(2.minutes) @@ sequential

}
