package nl.vroste.zio.kinesis.client

import io.github.vigoo.zioaws.kinesis
import io.github.vigoo.zioaws.kinesis.Kinesis
import io.github.vigoo.zioaws.kinesis.model.{
  CreateStreamRequest,
  DeleteStreamRequest,
  ListShardsRequest,
  PutRecordsRequest,
  PutRecordsRequestEntry,
  PutRecordsResponse,
  Shard
}
import nl.vroste.zio.kinesis.client.serde.Serializer
import software.amazon.awssdk.services.kinesis.model.{ ResourceInUseException, ResourceNotFoundException }
import zio.clock.Clock
import zio.console.{ putStrLn, Console }
import zio.duration._
import zio.{ Chunk, Schedule, ZIO, ZManaged }

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
    ((if (batchIndex == 1) 1 else (batchIndex - 1) * batchSize) to (batchSize * batchIndex) - 1)

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

}
