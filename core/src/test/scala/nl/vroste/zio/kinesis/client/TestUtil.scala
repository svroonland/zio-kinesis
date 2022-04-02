package nl.vroste.zio.kinesis.client

import nl.vroste.zio.kinesis.client.producer.ProducerMetrics
import nl.vroste.zio.kinesis.client.serde.{ Serde, Serializer }
import software.amazon.awssdk.services.kinesis.model.{ ResourceInUseException, ResourceNotFoundException }
import zio.Console.printLine
import zio.Schedule.WithState
import zio.aws.dynamodb.DynamoDb
import zio.aws.dynamodb.model.DeleteTableRequest
import zio.aws.dynamodb.model.primitives.TableName
import zio.aws.kinesis.Kinesis
import zio.aws.kinesis.model._
import zio.aws.kinesis.model.primitives._
import zio.stream.ZStream
import zio.{ Clock, Console, Random, _ }

import java.util.UUID
import scala.annotation.nowarn

object TestUtil {

  def withStream[R, A](name: String, shards: Int)(
    f: ZIO[R, Throwable, A]
  ): ZIO[Kinesis with Clock with Console with R, Throwable, A] =
    createStream(name, shards)
      .tapZIO(_ => getShards(name))
      .useDiscard(f)

  def withRandomStreamEnv[R, A](shards: Int = 2)(
    f: (String, String) => ZIO[R, Throwable, A]
  ): ZIO[Kinesis with DynamoDb with Clock with Console with Random with R, Throwable, A] =
    (for {
      streamName      <- Random.nextUUID.map("zio-test-stream-" + _.toString)
      applicationName <- Random.nextUUID.map("zio-test-" + _.toString)
      _               <- createStream(streamName, shards)
      _               <- getShards(streamName)
      _               <- ZIO.finalizer(DynamoDb.deleteTable(DeleteTableRequest(TableName(applicationName))).ignore)
    } yield (streamName, applicationName)).use { case (streamName, applicationName) =>
      f(streamName, applicationName).fork.flatMap(_.join)
    }

  def getShards(name: String): ZIO[Kinesis with Clock, Throwable, Chunk[Shard.ReadOnly]] =
    Kinesis
      .listShards(ListShardsRequest(streamName = Some(StreamName(name))))
      .mapError(_.toThrowable)
      .runCollect
      .filterOrElseWith(_.nonEmpty)(_ => getShards(name).delay(1.second))
      .catchSome { case _: ResourceInUseException => getShards(name).delay(1.second) }

  def createStream(
    streamName: String,
    nrShards: Int
  ): ZIO[Scope with Console with Clock with Kinesis, Throwable, Unit] = {
    createStreamUnmanaged(streamName, nrShards).toManagedWith(_ =>
      Kinesis
        .deleteStream(DeleteStreamRequest(StreamName(streamName), enforceConsumerDeletion = Some(BooleanObject(true))))
        .mapError(_.toThrowable)
        .catchSome { case _: ResourceNotFoundException =>
          ZIO.unit
        }
        .orDie
    )
  } <* ZIO.fromZIO(waitForStreamActive(streamName))

  def waitForStreamActive(streamName: String): ZIO[Kinesis with Clock, Throwable, Unit] =
    Kinesis
      .describeStream(DescribeStreamRequest(StreamName(streamName)))
      .mapError(_.toThrowable)
      .tap(r => Task(println(r)))
      .retryWhile {
        case _: ResourceNotFoundException => true
        case _                            => false
      }
      .map(_.streamDescription)
      .map(_.streamStatus)
      .delay(500.millis)
      .repeatUntilEquals(StreamStatus.ACTIVE)
      .unit

  @nowarn("msg=a type was inferred to be `Any`")
  def createStreamUnmanaged(
    streamName: String,
    nrShards: Int
  ): ZIO[Console with Clock with Kinesis, Throwable, Unit] =
    Kinesis
      .createStream(CreateStreamRequest(StreamName(streamName), Some(PositiveIntegerObject(nrShards))))
      .mapError(_.toThrowable)
      .catchSome { case _: ResourceInUseException =>
        printLine("Stream already exists").orDie
      }
      .retry(Schedule.exponential(1.second) && Schedule.recurs(10))

  @nowarn("msg=a type was inferred to be `Any`")
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
  ): ZIO[Random with Console with Clock with Kinesis with Any, Throwable, Unit] =
    Ref
      .make(ProducerMetrics.empty)
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

  val defaultChunkSize = 10000

  def putRecords[R, T](
    streamName: String,
    serializer: Serializer[R, T],
    records: Iterable[ProducerRecord[T]]
  ): ZIO[Kinesis with R, Throwable, PutRecordsResponse.ReadOnly] =
    for {
      recordsAndBytes <- ZIO.foreach(records)(r => serializer.serialize(r.data).map((_, r.partitionKey)))
      entries          = recordsAndBytes.map { case (data, partitionKey) =>
                           PutRecordsRequestEntry(Data(data), partitionKey = PartitionKey(partitionKey))
                         }
      response        <- Kinesis
                           .putRecords(PutRecordsRequest(entries.toList, StreamName(streamName)))
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
      .viaFunction(throttle(produceRate, _))
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
      ).buffer(produceRate * 10)
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
    }.rechunk(defaultChunkSize).take(nrRecords.toLong).viaFunction(throttle(produceRate, _)).buffer(defaultChunkSize)
    massProduceRecords(producer, records)
  }

  def massProduceRecords[R, T](
    producer: Producer[T],
    records: ZStream[R, Nothing, ProducerRecord[T]]
  ): ZIO[Clock with R, Throwable, Unit] =
    records
      .mapChunks(Chunk.single)
      .buffer(20)
      .mapZIOParUnordered(50) { chunk =>
        producer
          .produceChunk(chunk)
          .tapError(e => ZIO.logError(s"Producing records chunk failed, will retry: ${e}"))
          .retry(retryOnResourceNotFound && Schedule.recurs(1))
          .tapError(e => ZIO.logError(s"Producing records chunk failed, will retry: ${e}"))
          .retry(Schedule.exponential(1.second))
      }
      .runDrain
      .tapErrorCause(e => ZIO.logError(s"Producing records chunk failed: ${e.prettyPrint}")) *> // TODO log cause
      ZIO.logInfo("Producing records is done!")
}
