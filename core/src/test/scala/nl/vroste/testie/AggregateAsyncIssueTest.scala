package nl.vroste.testie

import nl.vroste.testie.AggregateAsyncIssueTest.TestProducer.batcher
import nl.vroste.zio.kinesis.client.TestUtil.{ createStream, createStreamUnmanaged, withStream }
import nl.vroste.zio.kinesis.client.localstack.LocalStackServices
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException
import zio.Console.printLine
import zio.aws.kinesis.Kinesis
import zio.aws.kinesis.model.primitives.StreamName
import zio.aws.kinesis.model.{ DeleteStreamRequest, DescribeStreamRequest, StreamStatus }
import zio.stream.{ ZSink, ZStream }
import zio.test.{ assertCompletes, TestAspect, ZIOSpecDefault }
import zio._

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

  def withStream[R, A](name: String, shards: Int)(
    f: ZIO[R, Throwable, A]
  ): ZIO[Kinesis with R, Throwable, A] =
    ZIO.scoped[Kinesis with R] {
      (createStream(name, shards)) *> f
    }

  def createStream(
    streamName: String,
    nrShards: Int
  ): ZIO[Scope with Kinesis, Throwable, Unit] =
    ZIO.acquireRelease(createStreamUnmanaged(streamName, nrShards))(_ =>
      Kinesis
        .deleteStream(DeleteStreamRequest(StreamName(streamName), enforceConsumerDeletion = Some(true)))
        .mapError(_.toThrowable)
        .catchSome { case _: ResourceNotFoundException =>
          ZIO.unit
        }
        .orDie
    )

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
