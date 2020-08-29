package nl.vroste.zio.kinesis.client

import java.util.UUID

import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.serde.Serde
import software.amazon.awssdk.services.kinesis.model.KinesisException
import zio.clock.Clock
import zio.console._
import zio.duration._
import zio.logging.slf4j.Slf4jLogger
import zio.logging.{ LogContext, LogWriter, Logging }
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._
import zio.{ Chunk, URIO, ZIO }

object ProducerTest extends DefaultRunnableSpec {
  import TestUtil._

  val loggingLayer = Slf4jLogger.make((_, logEntry) => logEntry, Some(getClass.getName))

  val myOwnLogger = Logging.make(new LogWriter[Any] {
    override def writeLog(
      context: LogContext,
      line: => String
    ): URIO[Any, Unit] = ???
  })

  val env = (LocalStackServices.localStackAwsLayer.orDie >>> (AdminClient.live ++ Client.live)).orDie >+>
    (loggingLayer ++ Clock.live)

  def spec =
    suite("Producer")(
      testM("produce a single record immediately") {

        val streamName = "zio-test-stream-producer"

        (for {
          _        <- putStrLn("Creating stream").toManaged_
          _        <- createStream(streamName, 1)
          _        <- putStrLn("Creating producer").toManaged_
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
      testM("produce records to Kinesis successfully and efficiently") {
        // This test demonstrates production of about 5000-6000 records per second on my Mid 2015 Macbook Pro

        val streamName = "zio-test-stream-" + UUID.randomUUID().toString

        (for {
          _        <- createStream(streamName, 10)
          producer <- Producer
                        .make(streamName, Serde.asciiString, ProducerSettings(bufferSize = 32768))
        } yield producer).use {
          producer =>
            for {
              _                <- ZIO.sleep(5.second)
              // Parallelism, but not infinitely (not sure if it matters)
              nrChunks          = 50
              nrRecordsPerChunk = 1000
              time             <- ZIO
                        .collectAllParN_(20)((1 to nrChunks).map { i =>
                          for {
                            _      <- putStrLn(s"Starting chunk $i")
                            records = (1 to nrRecordsPerChunk).map(j => ProducerRecord(s"key$i", s"message$i-$j"))
                            _      <- (producer
                                     .produceChunk(Chunk.fromIterable(records)) *> putStrLn(s"Chunk $i completed"))
                          } yield ()
                        })
                        .timed
                        .map(_._1)
              _                <- putStrLn(s"Produced ${nrChunks * nrRecordsPerChunk} in ${time.getSeconds}")
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
