package nl.vroste.zio.kinesis.client

import io.github.vigoo.zioaws.cloudwatch.CloudWatch
import io.github.vigoo.zioaws.dynamodb.DynamoDb
import io.github.vigoo.zioaws.kinesis
import io.github.vigoo.zioaws.kinesis.Kinesis
import io.github.vigoo.zioaws.kinesis.model.{ ScalingType, UpdateShardCountRequest }
import nl.vroste.zio.kinesis.client
import nl.vroste.zio.kinesis.client.localstack.LocalStackServices
import nl.vroste.zio.kinesis.client.producer.ProducerLive.ProduceRequest
import nl.vroste.zio.kinesis.client.producer.{ ProducerLive, ProducerMetrics, ShardMap }
import nl.vroste.zio.kinesis.client.serde.{ Serde, Serializer }
import software.amazon.awssdk.services.kinesis.model.KinesisException
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.{ putStrLn, Console }
import zio.duration._
import zio.logging.{ Logger, Logging }
import zio.random.Random
import zio.stream.{ ZStream, ZTransducer }
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._
import zio.{ system, Chunk, Queue, Ref, Runtime, ZIO, ZLayer, ZManaged }

import java.security.MessageDigest
import java.time.Instant
import java.util.UUID
import scala.annotation.unused

object ProducerTest extends DefaultRunnableSpec {
  import TestUtil._

  val loggingLayer = Logging.console() >>> Logging.withRootLoggerName(getClass.getName)

  val useAws = Runtime.default.unsafeRun(system.envOrElse("ENABLE_AWS", "0")).toInt == 1

  val env: ZLayer[
    Any,
    Nothing,
    CloudWatch with Kinesis with DynamoDb with Clock with Console with Logging with Random with Blocking
  ] =
    ((if (useAws) client.defaultAwsLayer else LocalStackServices.localStackAwsLayer()).orDie) >+>
      (Clock.live ++ zio.console.Console.live ++ Random.live ++ Blocking.live >+> loggingLayer)

  def spec =
    suite("Producer")(
      testM("produce a single record immediately") {

        val streamName = "zio-test-stream-producer"

        withStream(streamName, 1) {
          Producer
            .make(streamName, Serde.asciiString, ProducerSettings(bufferSize = 128))
            .use { producer =>
              for {
                _         <- putStrLn("Producing record!").orDie
                result    <- producer.produce(ProducerRecord("bla1", "bla1value")).timed
                (time, _)  = result
                _         <- putStrLn(time.toMillis.toString).orDie
                result2   <- producer.produce(ProducerRecord("bla1", "bla1value")).timed
                (time2, _) = result2
                _         <- putStrLn(time2.toMillis.toString).orDie
              } yield assertCompletes
            }
        }
      },
      testM("not block when passing an empty chunk to produceChunk") {

        val streamName = "zio-test-stream-producer-2"

        withStream(streamName, 1) {
          Producer
            .make(streamName, Serde.asciiString, ProducerSettings(bufferSize = 128))
            .use { producer =>
              for {
                _ <- producer.produceChunk(Chunk.empty)
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
                                metrics => totalMetrics.updateAndGet(_ + metrics).flatMap(m => putStrLn(m.toString).orDie)
                              )
          } yield producer).use { producer =>
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
                               _     <- putStrLn(s"Sending batch of size ${batch.size}!").orDie
                               times <- ZIO.foreachPar(batch)(producer.produce(_).timed.map(_._1))
                               _     <- timing.update(_ :+ times)
                             } yield ()
                           }
                           .runDrain
              results <- timing.get
              _       <- ZIO.foreach_(results)(r => putStrLn(r.map(_.toMillis).mkString(" ")).orDie)
            } yield assertCompletes
          }
        }
      } @@ TestAspect.ifEnvSet("ENABLE_AWS"),
      testM("produce records to Kinesis successfully and efficiently") {
        // This test demonstrates production of about 5000-6000 records per second on my Mid 2015 Macbook Pro

        val streamName = "zio-test-stream-producer4"

        Ref
          .make(ProducerMetrics.empty)
          .flatMap { totalMetrics =>
            (for {
              _        <- putStrLn("creating stream").orDie.toManaged_
              _        <- createStreamUnmanaged(streamName, 24).toManaged_
              _        <- putStrLn("creating producer").orDie.toManaged_
              producer <- Producer
                            .make(
                              streamName,
                              Serde.bytes,
                              ProducerSettings(
                                bufferSize = 16384 * 48,
                                maxParallelRequests = 24,
                                metricsInterval = 5.seconds,
                                aggregate = true
                              ),
                              metrics =>
                                totalMetrics
                                  .updateAndGet(_ + metrics)
                                  .flatMap(m =>
                                    putStrLn(
                                      s"""${metrics.toString}
                                         |${m.toString}""".stripMargin
                                    ).orDie
                                  )
                            )
            } yield producer).use { producer =>
              TestUtil.massProduceRecords(producer, 8000000, None, 14)
            } *> totalMetrics.get.flatMap(m => putStrLn(m.toString).orDie).as(assertCompletes)
          }
          .untraced
      } @@ timeout(5.minute) @@ TestAspect.ifEnvSet("ENABLE_AWS") @@ TestAspect.retries(3),
      testM("fail when attempting to produce to a stream that does not exist") {
        val streamName = "zio-test-stream-not-existing"

        Producer
          .make(streamName, Serde.asciiString, ProducerSettings(bufferSize = 32768))
          .use { producer =>
            val records = (1 to 10).map(j => ProducerRecord(s"key$j", s"message$j-$j"))
            producer
              .produceChunk(Chunk.fromIterable(records)) *> putStrLn(s"Chunk completed").orDie
          }
          .run
          .map(r => assert(r)(fails(isSubtype[KinesisException](anything))))
      } @@ timeout(1.minute),
      suite("aggregation")(
        testM("batch aggregated records per shard") {
          val batcher = aggregatingBatcherForProducerRecord(shardMap, Serde.asciiString)

          val nrRecords = 10
          val records   = (1 to nrRecords).map(j => ProducerRecord(UUID.randomUUID().toString, s"message$j-$j"))

          for {
            batches <- runTransducer(batcher, records)
          } yield assert(batches.size)(equalTo(1)) &&
            assert(batches.flatMap(Chunk.fromIterable).map(_.aggregateCount).sum)(equalTo(nrRecords))
        },
        testM("aggregate records up to the record size limit") {
          checkM(
            Gen
              .listOf(
                Gen.crossN(
                  Gen.stringBounded(1, 1024)(Gen.anyUnicodeChar),
                  Gen.stringBounded(1, 1024 * 10)(Gen.anyUnicodeChar)
                )(Tuple2.apply)
              )
              .filter(_.nonEmpty)
          ) { inputs =>
            val batcher = aggregatingBatcherForProducerRecord(shardMap, Serde.asciiString)

            val records = inputs.map { case (key, value) => ProducerRecord(key, value) }

            for {
              batches           <- runTransducer(batcher, records)
              recordPayloadSizes = batches.flatMap(Chunk.fromIterable).map(r => r.payloadSize)
            } yield assert(recordPayloadSizes)(forall(isLessThanEqualTo(ProducerLive.maxPayloadSizePerRecord)))
          }
        },
        testM("aggregate records up to the batch size limit") {
          val batcher = aggregatingBatcherForProducerRecord(shardMap, Serde.asciiString)

          val nrRecords = 100000
          val records   = (1 to nrRecords).map(j => ProducerRecord(UUID.randomUUID().toString, s"message$j-$j"))

          for {
            batches          <- runTransducer(batcher, records)
            batchPayloadSizes = batches.map(_.map(_.data.length).sum)
          } yield assert(batches.map(_.size))(forall(isLessThanEqualTo(ProducerLive.maxRecordsPerRequest))) &&
            assert(batchPayloadSizes)(forall(isLessThanEqualTo(ProducerLive.maxPayloadSizePerRequest)))
        }
        // TODO test that retries end up in the output
      ),
      testM("produce to the right shard when aggregating") {
        val nrRecords = 1000
        val records   = (1 to nrRecords).map(j => ProducerRecord(UUID.randomUUID().toString, s"message$j-$j"))

        val streamName = "test-stream-2"
        TestUtil.withStream(streamName, 20) {

          Ref
            .make(ProducerMetrics.empty)
            .flatMap { totalMetrics =>
              for {
                _          <- Producer
                                .make(
                                  streamName,
                                  Serde.asciiString,
                                  ProducerSettings(aggregate = true),
                                  metricsCollector = m => totalMetrics.update(_ + m)
                                )
                                .use(_.produceChunk(Chunk.fromIterable(records)) *> putStrLn(s"Chunk completed").orDie)
                endMetrics <- totalMetrics.get
              } yield assert(endMetrics.shardPredictionErrors)(isZero)
            }
        }
      },
      testM("count aggregated records correctly in metrics") {
        val nrRecords = 1000
        val records   = (1 to nrRecords).map(j => ProducerRecord(UUID.randomUUID().toString, s"message$j-$j"))

        val streamName = "test-stream-4"
        TestUtil.withStream(streamName, 20) {

          Ref
            .make(ProducerMetrics.empty)
            .flatMap { totalMetrics =>
              for {
                _          <- Producer
                                .make(
                                  streamName,
                                  Serde.asciiString,
                                  ProducerSettings(aggregate = true),
                                  metricsCollector = m => totalMetrics.update(_ + m)
                                )
                                .use(_.produceChunk(Chunk.fromIterable(records)))
                endMetrics <- totalMetrics.get
              } yield assert(endMetrics.nrRecordsPublished)(equalTo(nrRecords.toLong))
            }
        }
      },
      testM("dynamically throttle with more than one producer") {
        val streamName = "zio-test-stream-producer3"

        def makeProducer(
          workerId: String,
          totalMetrics: Ref[ProducerMetrics]
        ): ZManaged[Any with Console with Clock with Kinesis with Logging with Blocking, Throwable, Producer[
          Chunk[Byte]
        ]] =
          Producer
            .make(
              streamName,
              Serde.bytes,
              ProducerSettings(
                bufferSize = 16384 * 4,
                maxParallelRequests = 12,
                metricsInterval = 5.seconds
              ),
              metrics =>
                totalMetrics
                  .updateAndGet(_ + metrics)
                  .flatMap(m =>
                    putStrLn(
                      s"""${workerId}: ${metrics.toString}
                         |${workerId}: ${m.toString}""".stripMargin
                    ).orDie
                  )
            )
            .updateService[Logger[String]](l => l.named(workerId))

        val nrRecords = 200000

        (for {
          _          <- putStrLn("creating stream").orDie
          _          <- createStreamUnmanaged(streamName, 1)
          _          <- putStrLn("creating producer").orDie
          metrics    <- Ref.make(ProducerMetrics.empty)
          _          <- (makeProducer("producer1", metrics) zip makeProducer("producer2", metrics)).use { case (p1, p2) =>
                          for {
                            run1 <- TestUtil.massProduceRecords(p1, nrRecords / 2, Some(nrRecords / 100), 14).fork
                            run2 <- TestUtil.massProduceRecords(p2, nrRecords / 2, Some(nrRecords / 100), 14).fork
                            _    <- run1.join <&> run2.join
                          } yield ()
                        }
          _          <- putStrLn(metrics.toString).orDie
          endMetrics <- metrics.get
        } yield assert(endMetrics.successRate)(isGreaterThan(0.75))).untraced
      } @@ timeout(5.minute) @@ TestAspect.ifEnvSet("ENABLE_AWS"),
      testM("updates the shard map after a reshard is detected") {
        val nrRecords = 1000
        val records   = (1 to nrRecords).map(j => ProducerRecord(UUID.randomUUID().toString, s"message$j-$j"))

        val streamName = "test-stream-5"
        TestUtil.withStream(streamName, 2) {

          for {
            metrics     <- Queue.unbounded[ProducerMetrics]
            producerFib <- Producer
                             .make(
                               streamName,
                               Serde.asciiString,
                               ProducerSettings(aggregate = true, metricsInterval = 3.second),
                               metricsCollector = metrics.offer(_).unit
                             )
                             .use { producer =>
                               ZStream
                                 .fromIterable(records)
                                 .chunkN(10) // TODO Until https://github.com/zio/zio/issues/4190 is fixed
                                 .throttleShape(10, 1.second)(_.size.toLong)
                                 .mapM(producer.produce)
                                 .runDrain
                             }
                             .fork
            // Wait for one or more shard prediction errors and then wait for recovery: no more errors
            done        <- ZStream
                             .fromQueue(metrics)
                             .tap(m =>
                               putStrLn(s"Detected shard prediction errors: ${m.shardPredictionErrors}").orDie.when(
                                 m.shardPredictionErrors > 0
                               )
                             )
                             .dropWhile(_.shardPredictionErrors == 0)
                             .dropWhile(_.shardPredictionErrors != 0)
                             .take(3) // This is not quite '3 consecutive', but close enough
                             .runDrain
                             .fork
            _           <- ZIO.sleep(10.seconds)
            _           <- putStrLn("Resharding").orDie
            _           <- kinesis
                             .updateShardCount(UpdateShardCountRequest(streamName, 4, ScalingType.UNIFORM_SCALING))
                             .mapError(_.toThrowable)
            _           <- done.join race producerFib.join
            _            = println("Done!")
            _           <- producerFib.interrupt
          } yield assertCompletes
        }
      } @@ TestAspect.timeout(2.minute)
    ).provideCustomLayerShared(env) @@ sequential

  def aggregatingBatcherForProducerRecord[R, T](
    @unused shardMap: ShardMap,
    serializer: Serializer[R, T]
  ): ZTransducer[R with Logging, Throwable, ProducerRecord[T], Seq[ProduceRequest]] =
    ProducerLive
      .aggregator(MessageDigest.getInstance("MD5"))
      .contramapM((r: ProducerRecord[T]) =>
        ProducerLive.makeProduceRequest(r, serializer, Instant.now).map(_._2)
      ) >>> ProducerLive.batcher

  def runTransducer[R, E, I, O](parser: ZTransducer[R, E, I, O], input: Iterable[I]): ZIO[R, E, Chunk[O]] =
    ZStream.fromIterable(input).transduce(parser).runCollect

  val shardMap = ShardMap(
    Chunk(ShardMap.minHashKey, ShardMap.maxHashKey / 2 + 1),
    Chunk(ShardMap.maxHashKey / 2, ShardMap.maxHashKey),
    Chunk("001", "002"),
    Instant.now
  )
}
