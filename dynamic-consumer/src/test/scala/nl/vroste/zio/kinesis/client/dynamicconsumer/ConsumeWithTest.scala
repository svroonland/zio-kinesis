package nl.vroste.zio.kinesis.client.dynamicconsumer

import io.github.vigoo.zioaws.cloudwatch.CloudWatch
import io.github.vigoo.zioaws.dynamodb.DynamoDb
import io.github.vigoo.zioaws.kinesis.Kinesis
import nl.vroste.zio.kinesis.client.localstack.LocalStackServices
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.{ ProducerRecord, TestUtil }
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.{ putStrLn, Console }
import zio.logging.Logging
import zio.test.Assertion.equalTo
import zio.test.TestAspect.{ sequential, timeout }
import zio.test.{ assert, DefaultRunnableSpec }
import zio.{ Has, Promise, Ref, ZLayer }
import DynamicConsumer.consumeWith
import zio.duration.durationInt
import zio.random.Random
import zio.test.mock.MockRandom

import java.util.UUID

object ConsumeWithTest extends DefaultRunnableSpec {
  import TestUtil._

  val loggingLayer: ZLayer[Console with Clock, Nothing, Logging] =
    Logging.console() >>> Logging.withRootLoggerName(getClass.getName)

  private val env: ZLayer[
    Any,
    Throwable,
    Console
      with Clock
      with Blocking
      with Random
      with Logging
      with CloudWatch
      with Kinesis
      with DynamoDb
      with DynamicConsumer
  ] =
    (Console.live ++ Clock.live ++ Blocking.live ++ Random.live) >+> loggingLayer >+> LocalStackServices
      .localStackAwsLayer() >+> DynamicConsumer.live

  def testConsume1 =
    testM("consumeWith should consume records produced on all shards") {
      withRandomStreamEnv(2) { (streamName, applicationName) =>
        val nrRecords = 4

        for {
          refProcessed      <- Ref.make(Seq.empty[String])
          finishedConsuming <- Promise.make[Nothing, Unit]
          assert            <- {
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
                                    checkpointBatchSize = 2,
                                    configureKcl = _.withPolling
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
    }

  def testConsume2 =
    testM(
      "consumeWith should, after a restart due to a record processing error, consume records produced on all shards"
    ) {
      withRandomStreamEnv(2) { (streamName, applicationName) =>
        val nrRecords = 50
        val batchSize = 10L

        for {
          refProcessed      <- Ref.make(Seq.empty[String])
          finishedConsuming <- Promise.make[Nothing, Unit]
          assert            <- {
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
                                    checkpointBatchSize = batchSize,
                                    configureKcl = _.withPolling
                                  ) {
                                    FakeRecordProcessor
                                      .makeFailing(
                                        refProcessed,
                                        finishedConsuming,
                                        failFunction = (_: Any) == "msg31"
                                      )
                                  }.ignore
              _                <- putStrLn("Starting dynamic consumer - about to succeed")
              consumerFiber    <- consumeWith[Any, Logging, String](
                                    streamName,
                                    applicationName = applicationName,
                                    deserializer = Serde.asciiString,
                                    checkpointBatchSize = batchSize,
                                    configureKcl = _.withPolling
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
    }

  override def spec =
    suite("ConsumeWithTest")(
      testConsume1,
      testConsume2
    ).provideLayer(env.orDie) @@ timeout(7.minutes)

}
