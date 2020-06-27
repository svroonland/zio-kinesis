package nl.vroste.zio.kinesis.client

import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.TestUtil.retryOnResourceNotFound
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.zionative.leaserepository.DynamoDbLeaseRepository
import nl.vroste.zio.kinesis.client.zionative._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException
import software.amazon.kinesis.exceptions.ShutdownException
import zio._
import zio.console._
import zio.duration._
import zio.logging.slf4j.Slf4jLogger
import zio.logging.{ log, Logging }
import zio.stream.{ ZStream, ZTransducer }

/**
 * Example app that shows the ZIO-native and KCL workers running in parallel
 */
object TraceTest extends zio.App {
  val streamName      = "zio-test-stream-8" // + java.util.UUID.randomUUID().toString
  val nrRecords       = 2000000
  val nrShards        = 2
  val nrNativeWorkers = 1
  val nrKclWorkers    = 0
  val applicationName = "testApp-13"        // + java.util.UUID.randomUUID().toString(),
  val runtime         = 1.minute

  override def run(
    args: List[String]
  ): ZIO[zio.ZEnv, Nothing, ExitCode] = {
    for {
      _ <- createStreamUnmanaged("bla", 10)
      _ <- createStreamUnmanaged("bla", 10)
    } yield ExitCode.success
  }.foldCauseM(e => log.error(s"Program failed: ${e.prettyPrint}", e).as(ExitCode.failure), ZIO.succeed(_))
    .provideCustomLayer(ExampleApp.awsEnv)

  def createStreamUnmanaged(streamName: String, nrShards: Int): ZIO[Console with AdminClient, Throwable, Unit] =
    for {
      adminClient <- ZIO.service[AdminClient.Service]
      _           <- adminClient.createStream(streamName, nrShards)
    } yield ()
}
