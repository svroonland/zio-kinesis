package nl.vroste.zio.kinesis.client.dynamicconsumer

import nl.vroste.zio.kinesis.client.dynamicconsumer.DynamicConsumer.consumeWith
import nl.vroste.zio.kinesis.client.localstack.LocalStackServices
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.{ ProducerRecord, TestUtil }
import zio.Console.printLine
import zio.aws.cloudwatch.CloudWatch
import zio.aws.dynamodb.DynamoDb
import zio.aws.kinesis.Kinesis
import zio.logging.LogFormat
import zio.logging.backend.SLF4J
import zio.test.Assertion.equalTo
import zio.test.TestAspect.{ timeout, withLiveClock }
import zio.test.{ assert, ZIOSpecDefault }
import zio.{ durationInt, Promise, Ref, ZIO, ZLayer }

object ConsumeWithTest extends ZIOSpecDefault {
  import TestUtil._

  private val loggingLayer: ZLayer[Any, Nothing, Unit] = SLF4J
    .slf4j(
      format = LogFormat.colored
    )

  private val env: ZLayer[
    Any,
    Throwable,
    Any with CloudWatch with Kinesis with DynamoDb with DynamicConsumer
  ] =
    loggingLayer >+> LocalStackServices.localStackAwsLayer() >+> DynamicConsumer.live

  def testConsume1 =
    test("consumeWith should consume records produced on all shards") {
      withRandomStreamEnv(2) { (streamName, applicationName) =>
        val nrRecords = 4

        ZIO.scoped {
          for {
            refProcessed      <- Ref.make(Seq.empty[String])
            finishedConsuming <- Promise.make[Nothing, Unit]
            assert            <- {
              val records =
                (1 to nrRecords).map(i => ProducerRecord(s"key$i", s"msg$i"))
              (for {
                _                <- printLine("Putting records")
                _                <- putRecords(streamName, Serde.asciiString, records)
                                      .tapError(e => printLine(s"error1: $e"))
                                      .retry(retryOnResourceNotFound)
                _                <- printLine("Starting dynamic consumer")
                consumerFiber    <- consumeWith[Any, Any, String](
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
    }

  def testConsume2 =
    test(
      "consumeWith should, after a restart due to a record processing error, consume records produced on all shards"
    ) {
      withRandomStreamEnv(2) { (streamName, applicationName) =>
        val nrRecords = 50
        val batchSize = 10L

        ZIO.scoped {
          for {
            refProcessed      <- Ref.make(Seq.empty[String])
            finishedConsuming <- Promise.make[Nothing, Unit]
            assert            <- {
              val records =
                (1 to nrRecords).map(i => ProducerRecord(s"key$i", s"msg$i"))
              (for {
                _                <- printLine("Putting records")
                _                <- putRecords(streamName, Serde.asciiString, records)
                                      .tapError(e => printLine(s"error1: $e"))
                                      .retry(retryOnResourceNotFound)
                _                <- printLine("Starting dynamic consumer - about to fail")
                _                <- consumeWith[Any, Any, String](
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
                _                <- printLine("Starting dynamic consumer - about to succeed")
                consumerFiber    <- consumeWith[Any, Any, String](
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
    }

  override def spec =
    suite("ConsumeWithTest")(
      testConsume1,
      testConsume2
    ).provideLayer(env.orDie) @@ withLiveClock @@ timeout(7.minutes)

}
