package nl.vroste.zio.kinesis.client

import java.util.UUID

import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.serde.Serde
import software.amazon.awssdk.services.kinesis.model.{ ResourceInUseException, ResourceNotFoundException }
import zio.clock.Clock
import zio.duration._
import zio.test.TestAspect._
import zio.test._
import zio.{ Chunk, Schedule, ZIO }

object ProducerTest extends {
  private val retryOnResourceNotFound = Schedule.doWhile[Throwable] {
    case _: ResourceNotFoundException => true
    case _                            => false
  } &&
    Schedule.recurs(5) &&
    Schedule.exponential(2.second)

  val createStream = (streamName: String, nrShards: Int) =>
    for {
      adminClient <- AdminClient.create
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

} with DefaultRunnableSpec(
  suite("Producer")(
    testM("produce records to Kinesis successfully") {

      val streamName = "zio-test-stream-" + UUID.randomUUID().toString

      (for {
        _        <- createStream(streamName, 10)
        client   <- Client.create
        producer <- Producer.make(streamName, client, Serde.asciiString).provide(Clock.Live)
      } yield producer).use { producer =>
        println("Putting records")
        for {
          _ <- ZIO.traversePar(1 to 200) { i =>
                val records = (1 to 100).map(j => ProducerRecord(s"key${i}", s"message${i}-${j}"))
                producer.produceChunk(Chunk.fromIterable(records)) >>= (ZIO.traverse(_)(
                  result => ZIO(println(s"Record ${i} produced ${result}"))
                ))
              }
        } yield assertCompletes
      }
    }
  ) @@ timeout(1.minute) @@ sequential
)
