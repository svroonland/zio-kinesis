package nl.vroste.zio.kinesis.client
import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.serde.Serializer
import zio.clock.Clock
import zio.duration.{ Duration, _ }
import zio.stream.{ ZSink, ZStream }
import zio._

import scala.collection.JavaConverters._

/**
 * Higher level functionality over PutRecords
 *
 * Handles batching, record aggregation, retrying in case of failure due to Kinesis rate limits.
 *
 * Has an internal queue
 */
trait Producer[T] {
  def produce(r: ProducerRecord[T]): Task[Unit]
  def produceChunk(chunk: Chunk[ProducerRecord[T]]): Task[Unit]
}

object Producer {
  case class ProduceRequest[T](r: ProducerRecord[T], done: Promise[Throwable, Unit])

  def make[R, T](
    streamName: String,
    client: Client,
    serializer: Serializer[R, T],
    bufferSize: Int = 1024, // Prefer powers of 2
    maxBufferDuration: Duration = 100.millis
  ): ZManaged[R with Clock, Throwable, Producer[T]] = {

    // Make a putRecords call and return failed records
    def produce(records: Chunk[ProducerRecord[T]]): ZIO[R, Throwable, Chunk[ProducerRecord[T]]] =
      client.putRecords(streamName, serializer, records.toVector).map { response =>
        val failed = Chunk
          .fromIterable(
            response
              .records()
              .asScala
          )
          .zipWith(records)(Tuple2.apply)
          .collect { case (result, record) if result.errorCode() != null => record }

        failed
      }

    def produceWithRetry(records: Chunk[ProducerRecord[T]]) = produce(records).flatMap { failed =>
      produce(failed)
    }

    for {
      queue <- zio.Queue.bounded[ProduceRequest[T]](bufferSize).toManaged(_.shutdown)
      q     = queue.contramapM((r: ProducerRecord[T]) => Promise.make[Throwable, Unit].map(ProduceRequest(r, _)))

      _ <- ZStream
            .fromQueue(q)
            .aggregateAsyncWithin(
              ZSink.collectAllN[ProduceRequest[T]](bufferSize.toLong),
              Schedule.spaced(maxBufferDuration)
            ) // groupedWithin
            .mapM { requests =>
              // TODO it should retry together with the new incoming records
              produceWithRetry(Chunk.fromIterable(requests).map(_.r)) *> ZIO.traverse(requests.map(_.done))(
                _.succeed(())
              )
            }
            .runDrain
            .toManaged_
            .fork
    } yield new Producer[T] {
      override def produce(r: ProducerRecord[T]): Task[Unit] =
        for {
          done    <- Promise.make[Throwable, Unit]
          request = ProduceRequest(r, done)
          _       <- queue.offer(request)
          _       <- done.await
        } yield ()

      override def produceChunk(chunk: Chunk[ProducerRecord[T]]): Task[Unit] =
        ZIO
          .traverse(chunk.toVector)(r => Promise.make[Throwable, Unit].map(ProduceRequest(r, _)))
          .flatMap(requests => queue.offerAll(requests) *> ZIO.foreach_(requests)(_.done.await))
    }
  }
}
