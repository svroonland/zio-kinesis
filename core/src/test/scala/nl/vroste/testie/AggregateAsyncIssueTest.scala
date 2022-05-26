package nl.vroste.testie

import nl.vroste.testie.AggregateAsyncIssueTest.TestProducer.batcher
import nl.vroste.zio.kinesis.client.ProducerRecord
import nl.vroste.zio.kinesis.client.TestUtil.withStream
import nl.vroste.zio.kinesis.client.localstack.LocalStackServices
import nl.vroste.zio.kinesis.client.producer.ProducerLive.ProduceRequest
import nl.vroste.zio.kinesis.client.producer.PutRecordsBatch
import zio.Console.printLine
import zio.aws.kinesis.model.primitives.PartitionKey
import zio.stream.{ ZSink, ZStream }
import zio.test.{ assertCompletes, TestAspect, ZIOSpecDefault }
import zio.{ Chunk, Clock, Queue, Scope, Task, ZIO, ZPool }

import java.time.Instant

object AggregateAsyncIssueTest extends ZIOSpecDefault {

  case class TestProducer[T](
    queue: Queue[ProduceRequest],
    md5Pool: ZPool[Throwable, Unit]
  ) {
    val runloop: ZIO[Any, Nothing, Unit] =
      ZStream
        .fromQueue(queue, 10)
        .mapChunksZIO(chunk => ZIO.logInfo(s"Dequeued chunk of size ${chunk.size}").as(Chunk.single(chunk)))
        .mapZIO(addPredictedShardToRequestsChunk)
        .flattenChunks
        .aggregateAsync(batcher)
        .tap(batch => ZIO.debug(s"Got batch ${batch}"))
        .runDrain

    private def addPredictedShardToRequestsChunk(chunk: Chunk[ProduceRequest]) =
      ZIO.scoped {
        md5Pool.get.flatMap { _ =>
          ZIO.attempt(chunk)
        }
      }.tapErrorCause(e => ZIO.debug(e)).orDie

    def produce: Task[Unit] =
      queue
        .offer(
          ProduceRequest(Chunk.empty, PartitionKey("123"), _ => ZIO.unit, Instant.now, null, 1)
        ) *> ZIO.never.unit
  }

  object TestProducer {
    def make[T] = for {
      queue   <- ZIO.acquireRelease(Queue.bounded[ProduceRequest](1024))(_.shutdown)
      md5Pool <- ZPool.make(ZIO.unit, 2)
      producer = TestProducer[T](queue, md5Pool)
      _       <- producer.runloop.forkScoped
    } yield producer

    val batcher: ZSink[Any, Nothing, ProduceRequest, ProduceRequest, Chunk[ProduceRequest]] =
      ZSink
        .foldZIO(PutRecordsBatch.empty)(_.isWithinLimits) { (batch, record: ProduceRequest) =>
          ZIO.succeed(batch.add(record))
        }
        .map(_.entries)
  }

  override def spec = suite("AggregateAsync")(
    test("issue") {

      val streamName = "zio-test-stream-producer"

      withStream(streamName, 1) {
        TestProducer
          .make[String]
          .flatMap { producer =>
            for {
              _ <- printLine("Producing record!").orDie
              _ <- producer.produce.timed
            } yield assertCompletes
          }
      }
    }
  ).provideCustomLayer(LocalStackServices.localStackAwsLayer() ++ Scope.default) @@ TestAspect.withLiveClock
}
