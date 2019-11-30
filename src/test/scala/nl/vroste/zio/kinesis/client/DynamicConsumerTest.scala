package nl.vroste.zio.kinesis.client

import java.util.UUID

import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.serde.Serde
import software.amazon.awssdk.services.kinesis.model.{ ResourceInUseException, ResourceNotFoundException }
import zio.duration._
import zio.test._
import zio.{ Schedule, ZIO }

object DynamicConsumerTest extends {
  private val retryOnResourceNotFound = Schedule.doWhile[Throwable] {
    case _: ResourceNotFoundException => true
    case _                            => false
  } &&
    Schedule.recurs(3) &&
    Schedule.exponential(1.second)

  val createStream = (streamName: String) =>
    for {
      adminClient <- AdminClient.create
      _ <- adminClient
            .createStream(streamName, 2)
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

} with DefaultRunnableSpec(
  suite("DynamicConsumer")(
    testM("consume records produced on all shards produced on the stream") {

      val streamName      = "zio-test-stream-" + UUID.randomUUID().toString
      val applicationName = "zio-test-" + UUID.randomUUID().toString

      (Client.create <* createStream(streamName)).use {
        client =>
          println("Putting records")
          for {
            _ <- client
                  .putRecords(
                    streamName,
                    Serde.asciiString,
                    Seq(ProducerRecord("key1", "msg1"), ProducerRecord("key2", "msg2"))
                  )
                  .tapError(e => ZIO(println(s"Putrecors error: ${e}")))
                  .retry(retryOnResourceNotFound)

            _ = println("Starting dynamic consumer")
            _ <- DynamicConsumer
                  .shardedStream(
                    streamName,
                    applicationName = applicationName,
                    deserializer = Serde.asciiString
                  )
                  .flatMapPar(Int.MaxValue)(_._2.flattenChunks)
                  .take(2)
                  .tap(r => ZIO(println(s"Got record ${r}")) *> r.checkpoint)
                  .runCollect
          } yield assertCompletes
      }
    }
  ) @@ zio.test.TestAspect.timeout(1.minute)
)
