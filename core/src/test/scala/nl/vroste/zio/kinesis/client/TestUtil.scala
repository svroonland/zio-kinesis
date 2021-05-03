package nl.vroste.zio.kinesis.client

import java.util.UUID

import io.github.vigoo.zioaws.kinesis
import io.github.vigoo.zioaws.kinesis.Kinesis
import io.github.vigoo.zioaws.kinesis.model._
import nl.vroste.zio.kinesis.client.producer.ProducerMetrics
import nl.vroste.zio.kinesis.client.serde.{ Serde, Serializer }
import software.amazon.awssdk.services.kinesis.model.{ ResourceInUseException, ResourceNotFoundException }
import zio.clock.Clock
import zio.console.{ putStrLn, Console }
import zio.duration._
import zio.logging.{ log, Logging }
import zio.stream.ZStream
import zio._

import scala.util.Random

object TestUtil {

  def withStream[R, A](name: String, shards: Int)(
    f: ZIO[R, Throwable, A]
  ): ZIO[Kinesis with Clock with Console with R, Throwable, A] =
    createStream(name, shards)
      .tapM(_ => getShards(name))
      .use_(f)

  def getShards(name: String): ZIO[Kinesis with Clock, Throwable, Chunk[Shard.ReadOnly]]                          =
    kinesis
      .listShards(ListShardsRequest(streamName = Some(name)))
      .mapError(_.toThrowable)
      .runCollect
      .filterOrElse(_.nonEmpty)(_ => getShards(name).delay(1.second))
      .catchSome { case _: ResourceInUseException => getShards(name).delay(1.second) }

  def createStream(streamName: String, nrShards: Int): ZManaged[Console with Clock with Kinesis, Throwable, Unit] =
    createStreamUnmanaged(streamName, nrShards).toManaged(_ =>
      kinesis
        .deleteStream(DeleteStreamRequest(streamName, enforceConsumerDeletion = Some(true)))
        .mapError(_.toThrowable)
        .catchSome {
          case _: ResourceNotFoundException => ZIO.unit
        }
        .orDie
    )

  def createStreamUnmanaged(
    streamName: String,
    nrShards: Int
  ): ZIO[Console with Clock with Kinesis, Throwable, Unit] =
    kinesis
      .createStream(CreateStreamRequest(streamName, nrShards))
      .mapError(_.toThrowable)
      .catchSome {
        case _: ResourceInUseException =>
          putStrLn("Stream already exists")
      }
      .retry(Schedule.exponential(1.second) && Schedule.recurs(10))

  val retryOnResourceNotFound: Schedule[Clock, Throwable, ((Throwable, Long), Duration)] =
    Schedule.recurWhile[Throwable] {
      case _: ResourceNotFoundException => true
      case _                            => false
    } &&
      Schedule.recurs(5) &&
      Schedule.exponential(2.second)

  def recordsForBatch(batchIndex: Int, batchSize: Int): Seq[Int] =
    (if (batchIndex == 1) 1 else (batchIndex - 1) * batchSize) to (batchSize * batchIndex) - 1

  /**
   * Produces a lot of random records at a high rate
   */
  def produceRecords(
    streamName: String,
    nrRecords: Int,
    produceRate: Int,
    size: Int,
    producerSettings: ProducerSettings = ProducerSettings()
  ): ZIO[Any with Console with Clock with Kinesis with Logging, Throwable, Unit] =
    Ref
      .make(ProducerMetrics.empty)
      .toManaged_
      .flatMap { totalMetrics =>
        Producer
          .make(
            streamName,
            Serde.asciiString,
            producerSettings,
            metrics =>
              totalMetrics
                .updateAndGet(_ + metrics)
                .flatMap(m => putStrLn(s"""${metrics.toString}
                                          |Total metrics: ${m.toString}""".stripMargin))
          )
      }
      .use(massProduceRecords(_, nrRecords, produceRate, size))

  val chunkSize = 1000

  def putRecords[R, T](
    streamName: String,
    serializer: Serializer[R, T],
    records: Iterable[ProducerRecord[T]]
  ): ZIO[Kinesis with R, Throwable, PutRecordsResponse.ReadOnly] =
    for {
      recordsAndBytes <- ZIO.foreach(records)(r => serializer.serialize(r.data).map((_, r.partitionKey)))
      entries          = recordsAndBytes.map {
                  case (data, partitionKey) =>
                    PutRecordsRequestEntry(Chunk.fromByteBuffer(data), partitionKey = partitionKey)
                }
      response        <- kinesis
                    .putRecords(PutRecordsRequest(entries.toList, streamName))
                    .mapError(_.toThrowable)
    } yield response

  /**
   * Produces a lot of random records at some rate
   *
   * Keys are random UUIDs to get an equal distribution over the shards
   *
   * @param producer
   * @param nrRecords
   * @param produceRate
   * @param recordSize
   * @return
   */
  def massProduceRecords(
    producer: Producer[String],
    nrRecords: Int,
    produceRate: Int,
    recordSize: Int
  ): ZIO[Logging with Clock, Throwable, Unit] = {
    val intervals = 5

    ZStream
      .unfoldChunk(0)(i =>
        if ((i + 1) * chunkSize <= nrRecords)
          Some((Chunk.fromIterable((i * chunkSize) until ((i + 1) * chunkSize)), i + 1))
        else
          None
      )
      .throttleShape(produceRate.toLong / intervals, 1000.millis * (1.0 / intervals), produceRate.toLong / intervals)(
        _.size.toLong
      )
      .as(ProducerRecord(s"${UUID.randomUUID()}", Random.nextString(recordSize)))
      .buffer(produceRate * 10)
      .mapChunks(Chunk.single)
      .mapMParUnordered(200) { chunk =>
        producer
          .produceChunk(chunk)
          .retry(retryOnResourceNotFound && Schedule.recurs(1))
          .tapError(e => log.error(s"Producing records chunk failed, will retry: ${e}"))
          .retry(Schedule.exponential(1.second))
      }
      .runDrain
      .tapCause(e => log.error("Producing records chunk failed", e)) *>
      log.info("Producing records is done!")
  }
}
