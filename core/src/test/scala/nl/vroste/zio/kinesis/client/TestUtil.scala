package nl.vroste.zio.kinesis.client

import zio.aws.dynamodb.DynamoDb
import zio.aws.dynamodb.model.DeleteTableRequest
import zio.aws.{ dynamodb, kinesis }
import zio.aws.kinesis.Kinesis
import zio.aws.kinesis.model._
import nl.vroste.zio.kinesis.client.producer.ProducerMetrics
import nl.vroste.zio.kinesis.client.serde.{ Serde, Serializer }
import software.amazon.awssdk.services.kinesis.model.{ ResourceInUseException, ResourceNotFoundException }
import zio._
import zio.stream.ZStream

import java.util.UUID
import zio.{ Clock, Console, Has, Random }
import zio.Console.printLine
import zio.Schedule.WithState

object TestUtil {

  def withStream[R, A](name: String, shards: Int)(
    f: ZIO[R, Throwable, A]
  ): ZIO[Kinesis with Clock with Has[Console] with R, Throwable, A] =
    createStream(name, shards)
      .tapZIO(_ => getShards(name))
      .useDiscard(f)

  def withRandomStreamEnv[R, A](shards: Int = 2)(
    f: (String, String) => ZIO[R, Throwable, A]
  ): ZIO[Kinesis with DynamoDb with Clock with Has[Console] with Random with R, Throwable, A] =
    (for {
      streamName      <- Random.nextUUID.map("zio-test-stream-" + _.toString).toManaged
      applicationName <- Random.nextUUID.map("zio-test-" + _.toString).toManaged
      _               <- createStream(streamName, shards)
      _               <- getShards(streamName).toManaged
      _               <- ZManaged.finalizer(dynamodb.deleteTable(DeleteTableRequest(applicationName)).ignore)
    } yield (streamName, applicationName)).use {
      case (streamName, applicationName) => f(streamName, applicationName).fork.flatMap(_.join)
    }

  def getShards(name: String): ZIO[Kinesis with Clock, Throwable, Chunk[Shard.ReadOnly]] =
    kinesis
      .listShards(ListShardsRequest(streamName = Some(name)))
      .mapError(_.toThrowable)
      .runCollect
      .filterOrElseWith(_.nonEmpty)(_ => getShards(name).delay(1.second))
      .catchSome { case _: ResourceInUseException => getShards(name).delay(1.second) }

  def createStream(
    streamName: String,
    nrShards: Int
  ): ZManaged[Has[Console] with Clock with Kinesis, Throwable, Unit]                     = {
    createStreamUnmanaged(streamName, nrShards).toManagedWith(_ =>
      kinesis
        .deleteStream(DeleteStreamRequest(streamName, enforceConsumerDeletion = Some(true)))
        .mapError(_.toThrowable)
        .catchSome {
          case _: ResourceNotFoundException => ZIO.unit
        }
        .orDie
    )
  } <* ZManaged.fromZIO(waitForStreamActive(streamName))

  def waitForStreamActive(streamName: String): ZIO[Kinesis with Clock, Throwable, Unit] =
    kinesis
      .describeStream(DescribeStreamRequest(streamName))
      .mapError(_.toThrowable)
      .retryWhile {
        case _: ResourceNotFoundException => true
        case _                            => false
      }
      .flatMap(_.streamDescription)
      .flatMap(_.streamStatus)
      .delay(500.millis)
      .repeatUntilEquals(StreamStatus.ACTIVE)
      .unit

  def createStreamUnmanaged(
    streamName: String,
    nrShards: Int
  ): ZIO[Has[Console] with Clock with Kinesis, Throwable, Unit] =
    kinesis
      .createStream(CreateStreamRequest(streamName, nrShards))
      .mapError(_.toThrowable)
      .catchSome {
        case _: ResourceInUseException =>
          printLine("Stream already exists").orDie
      }
      .retry(Schedule.exponential(1.second) && Schedule.recurs(10))

  val retryOnResourceNotFound: WithState[((Unit, Long), Long), Any, Throwable, (Throwable, Long, zio.Duration)] =
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
    maxRecordSize: Int,
    producerSettings: ProducerSettings = ProducerSettings()
  ): ZIO[Random with Has[Console] with Clock with Kinesis with Any, Throwable, Unit] =
    Ref
      .make(ProducerMetrics.empty)
      .toManaged
      .flatMap { totalMetrics =>
        Producer
          .make(
            streamName,
            Serde.bytes,
            producerSettings,
            metrics =>
              totalMetrics
                .updateAndGet(_ + metrics)
                .flatMap(m => printLine(s"""${metrics.toString}
                                           |Total metrics: ${m.toString}""".stripMargin).orDie)
          )
      }
      .use(massProduceRecords(_, nrRecords, produceRate = Some(produceRate), maxRecordSize))

  val defaultChunkSize = 1000

  def putRecords[R, T](
    streamName: String,
    serializer: Serializer[R, T],
    records: Iterable[ProducerRecord[T]]
  ): ZIO[Kinesis with R, Throwable, PutRecordsResponse.ReadOnly] =
    for {
      recordsAndBytes <- ZIO.foreach(records)(r => serializer.serialize(r.data).map((_, r.partitionKey)))
      entries          = recordsAndBytes.map {
                  case (data, partitionKey) =>
                    PutRecordsRequestEntry(data, partitionKey = partitionKey)
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
   * @param maxRecordSize
   * @return
   */
  def massProduceRecords(
    producer: Producer[Chunk[Byte]],
    nrRecords: Int,
    produceRate: Option[Int] = None,
    maxRecordSize: Int
  ): ZIO[Clock with Random, Throwable, Unit] = {
    val value     = Chunk.fill(maxRecordSize)(0x01.toByte)
    val chunkSize = produceRate.fold(defaultChunkSize)(Math.min(_, defaultChunkSize))
    val chunk     = Chunk.fill(chunkSize)(
      ProducerRecord(UUID.randomUUID().toString, value)
    )

    val records = ZStream
      .repeatZIOChunk(UIO(chunk))
      .take(nrRecords.toLong)
      .via(throttle(produceRate, _))
    massProduceRecords(producer, records)
  }

  private def throttle[R, E, A](
    produceRate: Option[Int],
    s: ZStream[R with Clock, E, A]
  ): ZStream[R with Clock, E, A] = {
    val intervals = 10
    produceRate.fold(s) { produceRate =>
      s.throttleShape(
          produceRate.toLong / intervals,
          1000.millis * (1.0 / intervals),
          produceRate.toLong / intervals
        )(
          _.size.toLong
        )
        .buffer(produceRate * 10)
    }
  }

  def massProduceRecordsRandomKeyAndValueSize(
    producer: Producer[Chunk[Byte]],
    nrRecords: Int,
    produceRate: Option[Int] = None,
    maxRecordSize: Int
  ): ZIO[Clock with Random, Throwable, Unit] = {
    val records = ZStream.repeatZIO {
      for {
        key   <- Random
                 .nextIntBetween(1, 256)
                 .flatMap(keyLength =>
                   ZIO.replicateZIO(keyLength)(Random.nextIntBetween('\u0000', '\uD7FF').map(_.toChar)).map(_.mkString)
                 )
        value <- Random
                   .nextIntBetween(1, maxRecordSize)
                   .map(valueLength => Chunk.fromIterable(List.fill(valueLength)(0x01.toByte)))
      } yield ProducerRecord(key, value)
    }.rechunk(defaultChunkSize).take(nrRecords.toLong).via(throttle(produceRate, _)).buffer(defaultChunkSize)
    massProduceRecords(producer, records)
  }

  def massProduceRecords[R, T](
    producer: Producer[T],
    records: ZStream[R, Nothing, ProducerRecord[T]]
  ): ZIO[Clock with R, Throwable, Unit] =
    records
      .mapChunks(Chunk.single)
      .mapZIOParUnordered(50) { chunk =>
        producer
          .produceChunk(chunk)
          .tapError(e => ZIO.logError(s"Producing records chunk failed, will retry: ${e}"))
          .retry(retryOnResourceNotFound && Schedule.recurs(1))
          .tapError(e => ZIO.logError(s"Producing records chunk failed, will retry: ${e}"))
          .retry(Schedule.exponential(1.second))
      }
      .runDrain
      .tapErrorCause(e => ZIO.logError("Producing records chunk failed", e)) *>
      ZIO.logInfo("Producing records is done!")
}
