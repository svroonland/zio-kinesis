package nl.vroste.zio.kinesis.client

import io.github.vigoo.zioaws.dynamodb.DynamoDb
import io.github.vigoo.zioaws.dynamodb.model.DeleteTableRequest
import io.github.vigoo.zioaws.{ dynamodb, kinesis }
import io.github.vigoo.zioaws.kinesis.Kinesis
import io.github.vigoo.zioaws.kinesis.model._
import nl.vroste.zio.kinesis.client.producer.ProducerMetrics
import nl.vroste.zio.kinesis.client.serde.{ Serde, Serializer }
import software.amazon.awssdk.services.kinesis.model.{ ResourceInUseException, ResourceNotFoundException }
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.random.Random
import zio.console.{ putStrLn, Console }
import zio.duration._
import zio.logging.{ log, Logging }
import zio.stream.ZStream

import java.util.UUID

object TestUtil {

  def withStream[R, A](name: String, shards: Int)(
    f: ZIO[R, Throwable, A]
  ): ZIO[Kinesis with Clock with Console with R, Throwable, A] =
    createStream(name, shards)
      .tapM(_ => getShards(name))
      .use_(f)

  def withRandomStreamEnv[R, A](shards: Int = 2)(
    f: (String, String) => ZIO[R, Throwable, A]
  ): ZIO[Kinesis with DynamoDb with Clock with Console with Random with R, Throwable, A] =
    (for {
      streamName      <- random.nextUUID.map("zio-test-stream-" + _.toString).toManaged_
      applicationName <- random.nextUUID.map("zio-test-" + _.toString).toManaged_
      _               <- createStream(streamName, shards)
      _               <- getShards(streamName).toManaged_
      _               <- ZManaged.finalizer(dynamodb.deleteTable(DeleteTableRequest(applicationName)).ignore)
    } yield (streamName, applicationName)).use { case (streamName, applicationName) =>
      f(streamName, applicationName).fork.flatMap(_.join)
    }

  def getShards(name: String): ZIO[Kinesis with Clock, Throwable, Chunk[Shard.ReadOnly]] =
    kinesis
      .listShards(ListShardsRequest(streamName = Some(name)))
      .mapError(_.toThrowable)
      .runCollect
      .filterOrElse(_.nonEmpty)(_ => getShards(name).delay(1.second))
      .catchSome { case _: ResourceInUseException => getShards(name).delay(1.second) }

  def createStream(streamName: String, nrShards: Int): ZManaged[Console with Clock with Kinesis, Throwable, Unit] = {
    createStreamUnmanaged(streamName, nrShards).toManaged(_ =>
      kinesis
        .deleteStream(DeleteStreamRequest(streamName, enforceConsumerDeletion = Some(true)))
        .mapError(_.toThrowable)
        .catchSome { case _: ResourceNotFoundException =>
          ZIO.unit
        }
        .orDie
    )
  } <* ZManaged.fromEffect(waitForStreamActive(streamName))

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
  ): ZIO[Console with Clock with Kinesis, Throwable, Unit] =
    kinesis
      .createStream(CreateStreamRequest(streamName, Some(nrShards)))
      .mapError(_.toThrowable)
      .catchSome { case _: ResourceInUseException =>
        putStrLn("Stream already exists").orDie
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
    maxRecordSize: Int,
    producerSettings: ProducerSettings = ProducerSettings()
  ): ZIO[Random with Console with Clock with Kinesis with Logging with Blocking, Throwable, Unit] =
    Ref
      .make(ProducerMetrics.empty)
      .toManaged_
      .flatMap { totalMetrics =>
        Producer
          .make(
            streamName,
            Serde.bytes,
            producerSettings,
            metrics =>
              totalMetrics
                .updateAndGet(_ + metrics)
                .flatMap(m => putStrLn(s"""${metrics.toString}
                                          |Total metrics: ${m.toString}""".stripMargin).orDie)
          )
      }
      .use(massProduceRecords(_, nrRecords, produceRate = Some(produceRate), maxRecordSize))

  val defaultChunkSize = 1024

  def putRecords[R, T](
    streamName: String,
    serializer: Serializer[R, T],
    records: Iterable[ProducerRecord[T]]
  ): ZIO[Kinesis with R, Throwable, PutRecordsResponse.ReadOnly] =
    for {
      recordsAndBytes <- ZIO.foreach(records)(r => serializer.serialize(r.data).map((_, r.partitionKey)))
      entries          = recordsAndBytes.map { case (data, partitionKey) =>
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
  ): ZIO[Logging with Clock with Random, Throwable, Unit] = {
    val value     = Chunk.fill(maxRecordSize)(0x01.toByte)
    val chunkSize = produceRate.fold(defaultChunkSize)(Math.min(_, defaultChunkSize))
    val chunk     = Chunk.fill(chunkSize)(
      ProducerRecord(UUID.randomUUID().toString, value)
    )

    val records = ZStream
      .repeatEffectChunk(UIO(chunk))
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
      ).buffer(produceRate * 10)
    }
  }

  def massProduceRecordsRandomKeyAndValueSize(
    producer: Producer[Chunk[Byte]],
    nrRecords: Int,
    produceRate: Option[Int] = None,
    maxRecordSize: Int
  ): ZIO[Logging with Clock with Random, Throwable, Unit] = {
    val records = ZStream.repeatEffect {
      for {
        key   <- random
                   .nextIntBetween(1, 256)
                   .flatMap(keyLength =>
                     ZIO.replicateM(keyLength)(random.nextIntBetween('\u0000', '\uD7FF').map(_.toChar)).map(_.mkString)
                   )
        value <- random
                   .nextIntBetween(1, maxRecordSize)
                   .map(valueLength => Chunk.fromIterable(List.fill(valueLength)(0x01.toByte)))
      } yield ProducerRecord(key, value)
    }.chunkN(defaultChunkSize)
      .take(nrRecords.toLong)
      .via(stream => throttle[Random with Clock, Throwable, ProducerRecord[Chunk[Byte]]](produceRate, stream))
      .buffer(defaultChunkSize)
    massProduceRecords(producer, records)
  }

  def massProduceRecords[R, T](
    producer: Producer[T],
    records: ZStream[R, Throwable, ProducerRecord[T]]
  ): ZIO[Logging with Clock with R, Throwable, Unit] =
    records
      .mapChunks(Chunk.single)
      .mapMParUnordered(50) { chunk =>
        producer
          .produceChunk(chunk)
          .tapError(e => log.error(s"Producing records chunk failed, will retry: ${e}"))
          .retry(retryOnResourceNotFound && Schedule.recurs(1))
          .tapError(e => log.error(s"Producing records chunk failed, will retry: ${e}"))
          .retry(Schedule.exponential(1.second))
      }
      .runDrain
      .tapCause(e => log.error("Producing records chunk failed", e)) *>
      log.info("Producing records is done!")
}
