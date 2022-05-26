package nl.vroste.testie

import nl.vroste.testie.AggregateAsyncIssueTest.TestProducer.batcher
import nl.vroste.zio.kinesis.client.TestUtil.withStream
import nl.vroste.zio.kinesis.client.localstack.LocalStackServices
import zio.Console.printLine
import zio.stream.{ ZSink, ZStream }
import zio.test.{ assertCompletes, TestAspect, ZIOSpecDefault }
import zio.{ Chunk, Queue, Scope, Task, ZIO, ZPool }

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
        .aggregateAsync(batcher[ProduceRequest])
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
          ProduceRequest(Chunk.empty)
        ) *> ZIO.never.unit
  }

  final case class ProduceRequest(data: Chunk[Byte])

  object TestProducer {
    def make = for {
      queue   <- ZIO.acquireRelease(Queue.bounded[ProduceRequest](1024))(_.shutdown)
      md5Pool <- ZPool.make(ZIO.unit, 2)
      producer = TestProducer[String](queue, md5Pool)
      _       <- producer.runloop.forkScoped
    } yield producer

    def batcher[T]: ZSink[Any, Nothing, T, T, Chunk[T]] =
      ZSink
        .foldZIO(Chunk.empty[T])(_.size < 10) { (batch, record: T) =>
          ZIO.succeed(batch :+ record)
        }
  }

  override def spec = suite("AggregateAsync")(
    test("issue") {

      val streamName = "zio-test-stream-producer"

      withStream(streamName, 1) {
        TestProducer.make.flatMap { producer =>
          for {
            _ <- printLine("Producing record!").orDie
            _ <- producer.produce.timed
          } yield assertCompletes
        }
      }
    }
  ).provideCustomLayer(LocalStackServices.localStackAwsLayer() ++ Scope.default) @@ TestAspect.withLiveClock
}
