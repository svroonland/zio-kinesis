package nl.vroste.zio.kinesis.client

import nl.vroste.zio.kinesis.client
import nl.vroste.zio.kinesis.client.localstack.LocalStackServices
import nl.vroste.zio.kinesis.client.producer.ProducerLive.{ batcher, ProduceRequest }
import nl.vroste.zio.kinesis.client.producer.{ ProducerLive, ProducerMetrics, ShardMap }
import nl.vroste.zio.kinesis.client.serde.Serde
import software.amazon.awssdk.services.kinesis.model.KinesisException
import zio.Console.printLine
import zio.aws.cloudwatch.CloudWatch
import zio.aws.dynamodb.DynamoDb
import zio.aws.kinesis.Kinesis
import zio.aws.kinesis.model.primitives.{ PositiveIntegerObject, StreamName }
import zio.aws.kinesis.model.{ ScalingType, UpdateShardCountRequest }
import zio.stream.{ ZPipeline, ZStream }
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test.{ Gen, _ }
import zio.{ Chunk, Queue, Ref, ZIO, ZLayer, _ }

import java.security.MessageDigest
import java.time.Instant
import java.util.UUID

object ProducerTest extends ZIOSpecDefault {
  import TestUtil._

  val useAws = scala.sys.env.getOrElse("ENABLE_AWS", "0").toInt == 1

  val env: ZLayer[TestEnvironment, Nothing, CloudWatch with Kinesis with DynamoDb] =
    (if (useAws) client.defaultAwsLayer else LocalStackServices.localStackAwsLayer()).orDie

  def spec =
    suite("Producer")(
      test("produce a single record immediately") {

        val streamName = "zio-test-stream-producer"

        withStream(streamName, 1) {
          Producer
            .make(streamName, Serde.asciiString, ProducerSettings(bufferSize = 128))
            .flatMap { producer =>
              for {
                _         <- printLine("Producing record!").orDie
                result    <- producer.produce(ProducerRecord("bla1", "bla1value")).timed
                (time, _)  = result
                _         <- printLine(time.toMillis.toString).orDie
                result2   <- producer.produce(ProducerRecord("bla1", "bla1value")).timed
                (time2, _) = result2
                _         <- printLine(time2.toMillis.toString).orDie
              } yield assertCompletes
            }
        }
      },
      test("not block when passing an empty chunk to produceChunk") {

        val streamName = "zio-test-stream-producer-2"

        withStream(streamName, 1) {
          Producer
            .make(streamName, Serde.asciiString, ProducerSettings(bufferSize = 128))
            .flatMap { producer =>
              for {
                _ <- producer.produceChunk(Chunk.empty)
              } yield assertCompletes
            }
        }
      },
      test("produce records when prediction is disabled") {

        val streamName = "zio-test-stream-producer-3"

        withStream(streamName, 1) {
          Producer
            .make(streamName, Serde.asciiString, ProducerSettings(bufferSize = 128, shardPrediction = Producer.ShardPrediction.Disabled))
            .flatMap { producer =>
              for {
                _         <- printLine("Producing record!").orDie
                result    <- producer.produce(ProducerRecord("bla1", "bla1value")).timed
                (time, _)  = result
                _         <- printLine(time.toMillis.toString).orDie
                result2   <- producer.produce(ProducerRecord("bla1", "bla1value")).timed
                (time2, _) = result2
                _         <- printLine(time2.toMillis.toString).orDie
              } yield assertCompletes
            }
        }
      },
      test("support a ramp load") {
        val streamName = "zio-test-stream-producer-ramp"

        withStream(streamName, 10) {
          (for {
            totalMetrics <- Ref.make(ProducerMetrics.empty)
            producer     <- Producer
                              .make(
                                streamName,
                                Serde.asciiString,
                                ProducerSettings(bufferSize = 128),
                                metrics => totalMetrics.updateAndGet(_ + metrics).flatMap(m => printLine(m.toString).orDie)
                              )
          } yield producer).flatMap { producer =>
            val increment = 1
            for {
              batchSize <- Ref.make(1)
              timing    <- Ref.make[Chunk[Chunk[Duration]]](Chunk.empty)

              _       <- ZStream
                           .tick(1.second)
                           .take(200)
                           .mapZIO(_ => batchSize.getAndUpdate(_ + 1))
                           .map { batchSize =>
                             Chunk.fromIterable(
                               (1 to batchSize * increment)
                                 .map(i => ProducerRecord(s"key${i}" + UUID.randomUUID(), s"value${i}"))
                             )
                           }
                           .mapZIO { batch =>
                             for {
                               _     <- printLine(s"Sending batch of size ${batch.size}!").orDie
                               times <- ZIO.foreachPar(batch)(producer.produce(_).timed.map(_._1))
                               _     <- timing.update(_ :+ times)
                             } yield ()
                           }
                           .runDrain
              results <- timing.get
              _       <- ZIO.foreachDiscard(results)(r => printLine(r.map(_.toMillis).mkString(" ")).orDie)
            } yield assertCompletes
          }
        }
      } @@ TestAspect.ifEnvSet("ENABLE_AWS"),
      test("produce records to Kinesis successfully and efficiently") {
        val streamName = "zio-test-stream-producer4"

        Ref
          .make(ProducerMetrics.empty)
          .flatMap { totalMetrics =>
            (for {
              _        <- printLine("creating stream").orDie
              _        <- createStreamUnmanaged(streamName, 24)
              _        <- printLine("creating producer").orDie
              producer <- Producer
                            .make(
                              streamName,
                              Serde.bytes,
                              ProducerSettings(
                                bufferSize = 16384 * 48,
                                maxParallelRequests = 24,
                                metricsInterval = 5.seconds,
                                aggregation = Producer.Aggregation.ByPredictedShard()
                              ),
                              metrics =>
                                totalMetrics
                                  .updateAndGet(_ + metrics)
                                  .flatMap(m =>
                                    printLine(
                                      s"""${metrics.toString}
                                         |${m.toString}""".stripMargin
                                    ).orDie
                                  )
                            )
            } yield producer).flatMap { producer =>
              TestUtil.massProduceRecords(producer, 8000000, None, 14)
            } *> totalMetrics.get.flatMap(m => printLine(m.toString).orDie).as(assertCompletes)
          }
      } @@ timeout(5.minute) @@ TestAspect.ifEnvSet("ENABLE_AWS") @@ TestAspect.retries(3),
      test("fail when attempting to produce to a stream that does not exist") {
        val streamName = "zio-test-stream-not-existing"

        Producer
          .make(streamName, Serde.asciiString, ProducerSettings(bufferSize = 32768))
          .flatMap { producer =>
            val records = (1 to 10).map(j => ProducerRecord(s"key$j", s"message$j-$j"))
            producer
              .produceChunk(Chunk.fromIterable(records)) *> printLine(s"Chunk completed").orDie
          }
          .exit
          .map(r => assert(r)(fails(isSubtype[KinesisException](anything))))
      } @@ timeout(1.minute),
      suite("foldWhile")(
        test("empty")(
          assertZIO(
            ZStream.empty
              .transduce(
                ProducerLive.foldWhile(0)(_ => true)((x, y: Int) => ZIO.succeed(x + y))
              )
              .runCollect
          )(equalTo(Chunk(0)))
        ),
        test("short circuits") {
          val empty: ZStream[Any, Nothing, Int]     = ZStream.empty
          val single: ZStream[Any, Nothing, Int]    = ZStream.succeed(1)
          val double: ZStream[Any, Nothing, Int]    = ZStream(1, 2)
          val failed: ZStream[Any, String, Nothing] = ZStream.fail("Ouch")

          def run[E](stream: ZStream[Any, E, Int]) =
            (for {
              effects <- Ref.make[List[Int]](Nil)
              exit    <- stream
                           .transduce(ProducerLive.foldWhile(0)(_ => true) { (_, a: Int) =>
                             effects.update(a :: _) *> ZIO.succeed(30)
                           })
                           .runCollect
              result  <- effects.get
            } yield exit -> result).exit

          (assertZIO(run(empty))(succeeds(equalTo((Chunk(0), Nil)))) <*>
            assertZIO(run(single))(succeeds(equalTo((Chunk(30), List(1))))) <*>
            assertZIO(run(double))(succeeds(equalTo((Chunk(30), List(2, 1))))) <*>
            assertZIO(run(failed))(fails(equalTo("Ouch")))).map { case (r1, r2, r3, r4) =>
            r1 && r2 && r3 && r4
          }
        }
      ),
      suite("aggregation")(
        test("batch aggregated records per shard") {
          val batcher = aggregationPipeline(MessageDigest.getInstance("MD5"))

          val nrRecords = 10
          val records   = (1 to nrRecords).map(j => ProducerRecord(UUID.randomUUID().toString, s"message$j-$j"))

          for {
            batches <-
              ZStream
                .fromIterable(records)
                .mapZIO(r =>
                  ProducerLive
                    .makeProduceRequest(r, Serde.asciiString, Instant.now)
                    .map(_._2)
                )
                .via(batcher)
                .runCollect
          } yield assert(batches.size)(equalTo(1)) &&
            assert(batches.flatten.map(_.aggregateCount).sum)(equalTo(nrRecords)) &&
            assert(batches.headOption)(isSome(hasSize(equalTo(1))))
        },
        test("aggregate records up to the record size limit") {
          // TODO check or Gen appears to be very slow
          check(
            Gen
              .listOf(
                Gen.stringBounded(1, 1024)(Gen.unicodeChar) zip Gen.stringBounded(1, 1024 * 10)(Gen.unicodeChar)
              )
              .filter(_.nonEmpty)
          ) { inputs =>
            val batcher = aggregationPipeline(MessageDigest.getInstance("MD5"))

            val records = inputs.map { case (key, value) => ProducerRecord(key, value) }

            for {
              batches           <-
                ZStream
                  .fromIterable(records)
                  .mapZIO(r =>
                    ProducerLive
                      .makeProduceRequest(r, Serde.asciiString, Instant.now)
                      .map(_._2)
                  )
                  .via(batcher)
                  .runCollect
              recordPayloadSizes = batches.flatten.map(r => r.payloadSize)
            } yield assert(recordPayloadSizes)(forall(isLessThanEqualTo(ProducerLive.maxPayloadSizePerRecord)))
          }
        } @@ TestAspect.ignore,
        test("aggregate records up to the batch size limit") {
          val batcher = aggregationPipeline(MessageDigest.getInstance("MD5"))

          val nrRecords = 100000
          val records   = (1 to nrRecords).map(j => ProducerRecord(UUID.randomUUID().toString, s"message$j-$j"))

          for {
            batches          <-
              ZStream
                .fromIterable(records)
                .mapZIO(r =>
                  ProducerLive
                    .makeProduceRequest(r, Serde.asciiString, Instant.now)
                    .map(_._2)
                )
                .via(batcher)
                .runCollect
            batchPayloadSizes = batches.map(_.map(_.data.length).sum)
          } yield assert(batches.map(_.size))(forall(isLessThanEqualTo(ProducerLive.maxRecordsPerRequest))) &&
            assert(batchPayloadSizes)(forall(isLessThanEqualTo(ProducerLive.maxPayloadSizePerRequest)))
        }
        // TODO test that retries end up in the output
      ),
      test("produce to the right shard when aggregating") {
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
                                  ProducerSettings(aggregation = Producer.Aggregation.ByPredictedShard()),
                                  metricsCollector = m => totalMetrics.update(_ + m)
                                )
                                .flatMap(_.produceChunk(Chunk.fromIterable(records)) *> printLine(s"Chunk completed").orDie)
                endMetrics <- totalMetrics.get
              } yield assert(endMetrics.shardPredictionErrors)(isZero)
            }
        }
      },
      test("count aggregated records correctly in metrics") {
        val nrRecords = 1000
        val records   = (1 to nrRecords).map(j => ProducerRecord(UUID.randomUUID().toString, s"message$j-$j"))

        val streamName = "test-stream-4"
        TestUtil.withStream(streamName, 20) {

          Ref
            .make(ProducerMetrics.empty)
            .flatMap { totalMetrics =>
              for {
                _          <- ZIO.scoped {
                                Producer
                                  .make(
                                    streamName,
                                    Serde.asciiString,
                                    ProducerSettings(aggregation = Producer.Aggregation.ByPredictedShard()),
                                    metricsCollector = m => totalMetrics.update(_ + m)
                                  )
                                  .flatMap(_.produceChunk(Chunk.fromIterable(records)))
                              }
                endMetrics <- totalMetrics.get
              } yield assert(endMetrics.nrRecordsPublished)(equalTo(nrRecords.toLong))
            }
        }
      },
      test("dynamically throttle with more than one producer") {
        val streamName = "zio-test-stream-producer3"

        def makeProducer(
          workerId: String,
          totalMetrics: Ref[ProducerMetrics]
        ): ZIO[Scope with Kinesis, Throwable, Producer[
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
                    printLine(
                      s"""${workerId}: ${metrics.toString}
                         |${workerId}: ${m.toString}""".stripMargin
                    ).orDie
                  )
            )
            .logSpan(workerId)

        val nrRecords = 200000

        (for {
          _          <- printLine("creating stream").orDie
          _          <- createStreamUnmanaged(streamName, 1)
          _          <- printLine("creating producer").orDie
          metrics    <- Ref.make(ProducerMetrics.empty)
          _          <- (makeProducer("producer1", metrics) zip makeProducer("producer2", metrics)).flatMap { case (p1, p2) =>
                          for {
                            run1 <- TestUtil.massProduceRecords(p1, nrRecords / 2, Some(nrRecords / 100), 14).fork
                            run2 <- TestUtil.massProduceRecords(p2, nrRecords / 2, Some(nrRecords / 100), 14).fork
                            _    <- run1.join <&> run2.join
                          } yield ()
                        }
          _          <- printLine(metrics.toString).orDie
          endMetrics <- metrics.get
        } yield assert(endMetrics.successRate)(isGreaterThan(0.75)))
      } @@ timeout(5.minute) @@ TestAspect.ifEnvSet("ENABLE_AWS"),
      test("updates the shard map after a reshard is detected") {
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
                               ProducerSettings(aggregation = Producer.Aggregation.ByPredictedShard(), metricsInterval = 3.second),
                               metricsCollector = metrics.offer(_).unit
                             )
                             .flatMap { producer =>
                               ZStream
                                 .fromIterable(records)
                                 .rechunk(10) // TODO Until https://github.com/zio/zio/issues/4190 is fixed
                                 .throttleShape(10, 1.second)(_.size.toLong)
                                 .mapZIO(producer.produce)
                                 .runDrain
                             }
                             .fork
            // Wait for one or more shard prediction errors and then wait for recovery: no more errors
            done        <- ZStream
                             .fromQueue(metrics)
                             .tap(m =>
                               printLine(s"Detected shard prediction errors: ${m.shardPredictionErrors}").orDie.when(
                                 m.shardPredictionErrors > 0
                               )
                             )
                             .dropWhile(_.shardPredictionErrors == 0)
                             .dropWhile(_.shardPredictionErrors != 0)
                             .take(3) // This is not quite '3 consecutive', but close enough
                             .runDrain
                             .fork
            _           <- ZIO.sleep(10.seconds)
            _           <- printLine("Resharding").orDie
            _           <-
              Kinesis
                .updateShardCount(
                  UpdateShardCountRequest(StreamName(streamName), PositiveIntegerObject(4), ScalingType.UNIFORM_SCALING)
                )
                .mapError(_.toThrowable)
            _           <- done.join
            _            = println("Done!")
            _           <- producerFib.interrupt
          } yield assertCompletes
        }
      } @@ TestAspect.timeout(2.minute)
    ).provideLayer(env ++ Scope.default) @@
      sequential @@
      withLiveClock @@
      TestAspect.timeout(2.minute) @@
      TestAspect
        .fromLayer(Runtime.addLogger(ZLogger.default.map(println(_)).filterLogLevel(_ > LogLevel.Debug)))

  def aggregationPipeline(digest: MessageDigest): ZPipeline[Any, Nothing, ProduceRequest, Chunk[ProduceRequest]] = {
    val p1: ZPipeline[Any, Nothing, ProduceRequest, ProduceRequest] = ZPipeline
      .fromSink(ProducerLive.aggregator)
      .channel
      .mapOutZIO {
        _.mapZIO { b =>
          b.toProduceRequest(digest)
            .map(_.map(Chunk.single).getOrElse(Chunk.empty))
        }.map(_.flatten)
      }
      .toPipeline[ProduceRequest, ProduceRequest]

    p1 >>> ZPipeline.fromSink(batcher)
  }

  val shardMap = ShardMap(
    Chunk(ShardMap.minHashKey, ShardMap.maxHashKey / 2 + 1),
    Chunk(ShardMap.maxHashKey / 2, ShardMap.maxHashKey),
    Chunk("001", "002"),
    Instant.now
  )
}
