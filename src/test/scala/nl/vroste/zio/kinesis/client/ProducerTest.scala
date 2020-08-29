package nl.vroste.zio.kinesis.client

import java.util.UUID

import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.serde.Serde
import software.amazon.awssdk.services.kinesis.model.KinesisException
import zio.clock.Clock
import zio.console._
import zio.duration._
import zio.logging.slf4j.Slf4jLogger
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._
import zio.{ Chunk, Ref, ZIO }

object ProducerTest extends DefaultRunnableSpec {
  import TestUtil._

  val loggingLayer = Slf4jLogger.make((_, logEntry) => logEntry, Some(getClass.getName))

  val env = (LocalStackServices.localStackAwsLayer.orDie >>> (AdminClient.live ++ Client.live)).orDie >+>
    (loggingLayer ++ Clock.live)

//  val env = (client.defaultAwsLayer >>> (AdminClient.live ++ Client.live)).orDie >+>
//    (loggingLayer ++ Clock.live)

  def spec =
    suite("Producer")(
      testM("produce a single record immediately") {

        val streamName = "zio-test-stream-producer"

        (for {
          _        <- createStream(streamName, 1)
          producer <- Producer
                        .make(streamName, Serde.asciiString, ProducerSettings(bufferSize = 128))
        } yield producer).use { producer =>
          for {
            _         <- putStrLn("Producing record!")
            result    <- producer.produce(ProducerRecord("bla1", "bla1value")).timed
            (time, _)  = result
            _         <- putStrLn(time.toMillis.toString)
            result2   <- producer.produce(ProducerRecord("bla1", "bla1value")).timed
            (time2, _) = result2
            _         <- putStrLn(time2.toMillis.toString)
          } yield assertCompletes
        }
      },
      testM("support a ramp load") {
        val streamName = "zio-test-stream-producer-ramp"

        (for {
          _        <- createStreamUnmanaged(streamName, 10).toManaged_
          producer <- Producer
                        .make(streamName, Serde.asciiString, ProducerSettings(bufferSize = 128))
        } yield producer).use {
          producer =>
            val increment = 1
            for {
              batchSize <- Ref.make(1)
              timing    <- Ref.make[Chunk[Chunk[Duration]]](Chunk.empty)

              _       <- ZStream
                     .tick(1.second)
                     .take(200)
                     .mapM(_ => batchSize.getAndUpdate(_ + 1))
                     .map { batchSize =>
                       Chunk.fromIterable(
                         (1 to batchSize * increment).map(i =>
                           ProducerRecord(s"key${i}" + UUID.randomUUID(), s"value${i}")
                         )
                       )
                     }
                     .mapM { batch =>
                       for {
                         _     <- putStrLn(s"Sending batch of size ${batch.size}!")
                         times <- ZIO.foreachPar(batch)(producer.produce(_).timed.map(_._1))
                         _     <- timing.update(_.appended(times))
                         _     <-
                           putStrLn(s"Batch size ${batch.size}: avg = ${times.map(_.toMillis).sum / times.length} ms")
                       } yield ()
                     }
                     .runDrain
              results <- timing.get
              _       <- ZIO.foreach(results)(r => putStrLn(r.map(_.toMillis).mkString(" ")))
            } yield assertCompletes
        }
      },
      testM("produce records to Kinesis successfully and efficiently") {
        // This test demonstrates production of about 5000-6000 records per second on my Mid 2015 Macbook Pro

        val streamName = "zio-test-stream-producer2"

        (for {
          _        <- putStrLn("creating stream").toManaged_
          _        <- createStreamUnmanaged(streamName, 50).toManaged_
          _        <- putStrLn("creating producer").toManaged_
          producer <- Producer
                        .make(streamName, Serde.asciiString, ProducerSettings(bufferSize = 32768))
        } yield producer).use {
          producer =>
            for {
              _                <- ZIO.sleep(5.second)
              // Parallelism, but not infinitely (not sure if it matters)
              nrChunks          = 100
              nrRecordsPerChunk = 8000
              time             <- ZIO
                        .collectAllParN_(3)((1 to nrChunks).map { i =>
                          for {
                            _      <- putStrLn(s"Starting chunk $i")
                            records = (1 to nrRecordsPerChunk).map(j =>
                                        // Random UUID to get an even distribution over the shards
                                        ProducerRecord(s"key$i" + UUID.randomUUID(), s"message$i-$j")
                                      )
                            _      <- (producer.produceChunk(Chunk.fromIterable(records)) *> putStrLn(s"Chunk $i completed"))
                          } yield ()
                        })
                        .timed
                        .map(_._1)
              _                <- putStrLn(s"Produced ${nrChunks * nrRecordsPerChunk * 1000.0 / time.toMillis}")
            } yield assertCompletes
        }.untraced
      } @@ timeout(5.minute),
      testM("fail when attempting to produce to a stream that does not exist") {
        val streamName = "zio-test-stream-not-existing"

        (for {
          producer <- Producer
                        .make(streamName, Serde.asciiString, ProducerSettings(bufferSize = 32768))
        } yield producer).use { producer =>
          val records = (1 to 10).map(j => ProducerRecord(s"key$j", s"message$j-$j"))
          producer
            .produceChunk(Chunk.fromIterable(records)) *> putStrLn(s"Chunk completed")
        }.run.map(r => assert(r)(fails(isSubtype[KinesisException](anything))))
      } @@ timeout(1.minute)
    ).provideCustomLayerShared(env) @@ sequential
}
