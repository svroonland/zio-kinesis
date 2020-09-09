package nl.vroste.zio.kinesis.client

import java.time.Instant
import java.util.UUID

import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.ProducerLive.{ ProduceRequest, ShardMap }
import nl.vroste.zio.kinesis.client.serde.{ Serde, Serializer }
import software.amazon.awssdk.services.kinesis.model.KinesisException
import zio.clock.Clock
import zio.console.putStrLn
import zio.duration._
import zio.logging.Logging
import zio.logging.slf4j.Slf4jLogger
import zio.stream.{ ZStream, ZTransducer }
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._
import zio.{ Chunk, Ref, ZIO }

object ProducerTest extends DefaultRunnableSpec {
  import TestUtil._

  val loggingLayer = Slf4jLogger.make((_, logEntry) => logEntry, Some(getClass.getName))

  val env = (LocalStackServices.localStackAwsLayer.orDie >>> (AdminClient.live ++ Client.live)).orDie >+>
    (loggingLayer ++ Clock.live ++ zio.console.Console.live)

  def spec =
    suite("Producer")(
      testM("produce a single record immediately") {

        val streamName = "zio-test-stream-producer"

        withStream(streamName, 1) {
          Producer
            .make(streamName, Serde.asciiString, ProducerSettings(bufferSize = 128))
            .use { producer =>
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
        }
      },
      testM("support a ramp load") {
        val streamName = "zio-test-stream-producer-ramp"

        withStream(streamName, 10) {
          (for {
            totalMetrics <- Ref.make(ProducerMetrics.empty).toManaged_
            producer     <- Producer
                          .make(
                            streamName,
                            Serde.asciiString,
                            ProducerSettings(bufferSize = 128),
                            metrics => totalMetrics.updateAndGet(_ + metrics).flatMap(m => putStrLn(m.toString))
                          )
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
                           (1 to batchSize * increment)
                             .map(i => ProducerRecord(s"key${i}" + UUID.randomUUID(), s"value${i}"))
                         )
                       }
                       .mapM { batch =>
                         for {
                           _     <- putStrLn(s"Sending batch of size ${batch.size}!")
                           times <- ZIO.foreachPar(batch)(producer.produce(_).timed.map(_._1))
                           _     <- timing.update(_ :+ times)
                         } yield ()
                       }
                       .runDrain
                results <- timing.get
                _       <- ZIO.foreach(results)(r => putStrLn(r.map(_.toMillis).mkString(" ")))
              } yield assertCompletes
          }
        }
      } @@ TestAspect.ignore,
      testM("produce records to Kinesis successfully and efficiently") {
        // This test demonstrates production of about 5000-6000 records per second on my Mid 2015 Macbook Pro

        val streamName = "zio-test-stream-producer3"

        (for {
          _            <- putStrLn("creating stream").toManaged_
          _            <- createStreamUnmanaged(streamName, 5).toManaged_
          _            <- putStrLn("creating producer").toManaged_
          totalMetrics <- Ref.make(ProducerMetrics.empty).toManaged_
          producer     <- Producer
                        .make(
                          streamName,
                          Serde.asciiString,
                          ProducerSettings(bufferSize = 16384),
                          metrics => totalMetrics.updateAndGet(_ + metrics).flatMap(m => putStrLn(m.toString))
                        )
        } yield (producer, totalMetrics)).use {
          case (producer, totalMetrics) =>
            for {
              _                <- ZIO.sleep(5.second).when(false)
              // Parallelism, but not infinitely (not sure if it matters)
              nrChunks          = 100
              nrRecordsPerChunk = 8000
              time             <- ZIO
                        .collectAllParN_(3)((1 to nrChunks).map { i =>
                          val records = (1 to nrRecordsPerChunk).map(j =>
                            // Random UUID to get an even distribution over the shards
                            ProducerRecord(s"key$i" + UUID.randomUUID(), s"message$i-$j")
                          )

                          for {
                            //                            _      <- putStrLn(s"Starting chunk $i")
                            _ <- producer.produceChunk(Chunk.fromIterable(records))
                            _ <- putStrLn(s"Chunk $i completed")
                          } yield ()
                        })
                        .timed
                        .map(_._1)
              _                <- putStrLn(s"Produced ${nrChunks * nrRecordsPerChunk * 1000.0 / time.toMillis}")
              _                <- totalMetrics.get.flatMap(m => putStrLn(m.toString))
            } yield assertCompletes
        }.untraced
      } @@ timeout(5.minute) @@ TestAspect.ignore,
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
      } @@ timeout(1.minute),
      suite("aggregation")(
        testM("batch aggregated records") {
          val shardMap = ShardMap(Seq(("001", ShardMap.minHashKey, ShardMap.maxHashKey)), Instant.now)
          val batcher  = batcherForProducerRecord(shardMap, Serde.asciiString)

          val records = (1 to 10).map(j => ProducerRecord(s"key$j", s"message$j-$j"))

          for {
            batches <- runTransducer(batcher, records)
          } yield assert(batches.size)(equalTo(1))
        }
      )
    ).provideCustomLayerShared(env) @@ sequential

  def batcherForProducerRecord[R, T](
    shardMap: ShardMap,
    serializer: Serializer[R, T]
  ): ZTransducer[R with Logging, Throwable, ProducerRecord[T], Seq[ProduceRequest]] =
    ProducerLive.aggregatingBatcher.contramapM((r: ProducerRecord[T]) =>
      ProducerLive.makeProduceRequest(r, serializer, Instant.now, shardMap)
    )

  def runTransducer[R, E, I, O](parser: ZTransducer[R, E, I, O], input: Iterable[I]): ZIO[R, E, Chunk[O]] =
    ZStream.fromIterable(input).transduce(parser).runCollect
}
