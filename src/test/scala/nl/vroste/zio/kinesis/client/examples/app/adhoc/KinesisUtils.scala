package nl.vroste.zio.kinesis.client.examples.app.adhoc

import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.{ AdminClient, Client }
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest
import software.amazon.awssdk.services.kinesis.model.{ ResourceInUseException, ResourceNotFoundException }
import zio.clock._
import zio.duration._
import zio.stream.ZStream
import zio.{ Schedule, UIO, ZIO, ZManaged }

object KinesisUtils {

  private val retryOnResourceNotFound = Schedule.doWhile[Throwable] {
    case _: ResourceNotFoundException => true
    case _                            => false
  } &&
    Schedule.recurs(5) &&
    Schedule.exponential(2.second)

  val createStream: (String, Int) => ZManaged[KinesisLogger, Throwable, Unit] = (streamName: String, nrShards: Int) =>
    for {
      adminClient <- AdminClient.create
      _           <- adminClient
             .createStream(streamName, nrShards)
             .catchSome {
               case _: ResourceInUseException =>
                 info("Stream already exists")
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

  def recordAsString(i: Int) = s"""{ "id": "$i" }"""

  def putRecordsEmitter(
    streamName: String,
    batchSize: Int,
    max: Int,
    client: Client
  ): ZStream[KinesisLogger with Clock, Throwable, Int] =
    ZStream.unfoldM(1) { i =>
      if (i < max) {
        val recordsBatch = (i until i + batchSize).map(i => ProducerRecord(s"key$i", recordAsString(i)))
        val putRecordsM  = client
          .putRecords(
            streamName,
            Serde.asciiString,
            recordsBatch
          )
          .retry(retryOnResourceNotFound)
        for {
          _ <- info(s"i=$i putting $batchSize records into Kinesis")
          _ <- sleep(0.milliseconds)
          _ <- putRecordsM
        } yield Some((i, i + batchSize))
      } else
        ZIO.succeed(None)
    }

  def mgdDynamoDbTableCleanUp(appName: String): ZManaged[Any, Nothing, DynamoDbAsyncClient] =
    ZManaged.make(UIO(DynamoDbAsyncClient.builder().build())) { client =>
      val delete = client.deleteTable(DeleteTableRequest.builder.tableName(appName).build())
      ZIO.fromCompletionStage(delete).ignore *> ZIO.effect(client.close()).ignore
    }

}
