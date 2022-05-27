package nl.vroste.testie

import software.amazon.awssdk.services.kinesis.model.{ ResourceInUseException, ResourceNotFoundException }
import zio.Console.printLine
import zio._
import zio.aws.core.config.AwsConfig
import zio.aws.kinesis.Kinesis
import zio.aws.kinesis.model.primitives.{ PositiveIntegerObject, StreamName }
import zio.aws.kinesis.model.{ CreateStreamRequest, DeleteStreamRequest }
import zio.aws.netty.NettyHttpClient
import zio.stream.{ ZSink, ZStream }
import zio.test.{ assertCompletes, TestAspect, ZIOSpecDefault }

import java.net.URI

object AggregateAsyncIssueTest extends ZIOSpecDefault {

  case class TestProducer[T](
    queue: Queue[ProduceRequest],
    md5Pool: ZPool[Throwable, Unit]
  ) {
    val runloop: ZIO[Any, Throwable, Unit] =
      ZStream
        .fromQueue(queue, 10)
        .tap(_ => ZIO.logInfo(s"Dequeued element"))
        .mapZIO(addPredictedShardToRequestsChunk)
        .aggregateAsync(batcher[ProduceRequest])
        .tap(_ => ZIO.debug(s"Got past aggregateAsync"))
        .runDrain

    private def addPredictedShardToRequestsChunk(r: ProduceRequest) =
      ZIO.scoped {
        md5Pool.get *> ZIO.attempt(r)
      }

    def produce: Task[Unit] =
      queue.offer(ProduceRequest(Chunk.empty)) *> ZIO.never.unit
  }

  final case class ProduceRequest(data: Chunk[Byte])

  object TestProducer {
    def make = for {
      queue   <- ZIO.acquireRelease(Queue.bounded[ProduceRequest](1024))(_.shutdown)
      md5Pool <- ZPool.make(ZIO.unit, 2)
      producer = TestProducer[String](queue, md5Pool)
      _       <- producer.runloop.forkScoped
    } yield producer
  }

  def batcher[T]: ZSink[Any, Nothing, T, T, Chunk[T]] =
    ZSink
      .foldZIO(Chunk.empty[T])(_.size < 10) { (batch, record: T) =>
        ZIO.succeed(batch :+ record)
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

  def createStreamUnmanaged(
    streamName: String,
    nrShards: Int
  ): ZIO[Kinesis, Throwable, Unit] =
    Kinesis
      .createStream(CreateStreamRequest(StreamName(streamName), Some(PositiveIntegerObject(nrShards))))
      .mapError(_.toThrowable)
      .catchSome { case _: ResourceInUseException =>
        println("STREAM ALREADY EXISTS!")
        ZIO.unit
      }

  override def spec = suite("AggregateAsync")(
    test("issue") {

      val streamName = "zio-test-stream-producer4"

      withStream(streamName, 1) {
        TestProducer.make.flatMap { producer =>
          for {
            _ <- printLine("Producing record!")
            _ <- producer.produce
          } yield assertCompletes
        }
      }
    }
  ).provideCustomLayer(
    NettyHttpClient.default >>>
      AwsConfig.default >>>
      Kinesis.customized(_.endpointOverride(URI.create("http://localhost:4566"))) ++
      Scope.default
  ) @@ TestAspect.withLiveClock

}
