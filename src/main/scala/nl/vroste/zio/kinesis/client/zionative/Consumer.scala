package nl.vroste.zio.kinesis.client.zionative

import nl.vroste.zio.kinesis.client.AdminClient.StreamDescription
import nl.vroste.zio.kinesis.client.Client.{ ConsumerRecord, ShardIteratorType }
import nl.vroste.zio.kinesis.client.DynamicConsumer.{ Checkpointer, Record }
import nl.vroste.zio.kinesis.client.serde.Deserializer
import nl.vroste.zio.kinesis.client.zionative.FetchMode.{ EnhancedFanOut, Polling }
import nl.vroste.zio.kinesis.client.zionative.dynamodb.DynamoDbLeaseCoordinator
import nl.vroste.zio.kinesis.client.{ AdminClient, Client, Util }
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.model.{
  KmsThrottlingException,
  LimitExceededException,
  ProvisionedThroughputExceededException,
  Shard,
  Record => KinesisRecord
}
import zio._
import zio.clock.Clock
import zio.duration._
import zio.stream.ZStream
import zio.blocking.Blocking

sealed trait FetchMode
object FetchMode {

  /**
   * Fetches data in a polling manner
   *
   * @param batchSize The maximum number of records to retrieve in one call to GetRecords. Note that Kinesis
   *        defines limits in terms of the maximum size in bytes of this call, so you need to take into account
   *        the distribution of data size of your records (i.e. avg and max).
   * @param delay How long to wait after polling returned no new records
   * @param backoff When getting a Provisioned Throughput Exception or KmsThrottlingException, schedule to apply for backoff
   */
  case class Polling(
    batchSize: Int = 100,
    delay: Duration = 1.second,
    backoff: Schedule[Clock, Throwable, Any] = Util.exponentialBackoff(1.second, 1.minute)
  ) extends FetchMode

  /**
   * Fetch data using enhanced fanout
   */
  case object EnhancedFanOut extends FetchMode
}

trait Fetcher {
  def shardRecordStream(shard: Shard, startingPosition: ShardIteratorType): ZStream[Clock, Throwable, ConsumerRecord]
}

object Fetcher {
  def apply(f: (Shard, ShardIteratorType) => ZStream[Clock, Throwable, ConsumerRecord]): Fetcher =
    (shard, startingPosition) => f(shard, startingPosition)
}

trait LeaseCoordinator {
  def makeCheckpointer(shard: Shard): ZIO[Any, Throwable, Checkpointer]
}

object Consumer {
  def shardedStream[R, T](
    client: Client,
    adminClient: AdminClient,
    dynamoClient: DynamoDbAsyncClient,
    streamName: String,
    applicationName: String,
    deserializer: Deserializer[R, T],
    fetchMode: FetchMode = FetchMode.Polling()
  ): ZStream[
    Blocking with Clock,
    Throwable,
    (String, ZStream[R with Blocking with Clock, Throwable, Record[T]], Checkpointer)
  ] = {

    def toRecord(
      shardId: String,
      r: ConsumerRecord
    ): ZIO[R, Throwable, Record[T]] =
      deserializer.deserialize(r.data.asByteBuffer()).map { data =>
        Record(
          shardId,
          r.sequenceNumber,
          r.approximateArrivalTimestamp,
          data,
          r.partitionKey,
          r.encryptionType,
          0,    // r.subSequenceNumber,
          "",   // r.explicitHashKey,
          false //r.aggregated,
        )
      }

    val currentShards: ZStream[Clock, Throwable, Shard] = client.listShards(streamName)

    def makeFetcher(streamDescription: StreamDescription): ZManaged[Clock, Throwable, Fetcher] =
      fetchMode match {
        case c: Polling     => PollingFetcher.make(client, streamDescription, c)
        case EnhancedFanOut => EnhancedFanOutFetcher.make(client, streamDescription, applicationName)
      }

    ZStream.unwrapManaged {
      for {
        streamDescription <- adminClient.describeStream(streamName).toManaged_
        leaseCoordinator  <- DynamoDbLeaseCoordinator.make(dynamoClient, applicationName)
        fetcher           <- makeFetcher(streamDescription)
      } yield currentShards.mapM { shard =>
        val startingPosition = ShardIteratorType.TrimHorizon

        val shardStream = fetcher.shardRecordStream(shard, startingPosition).mapChunksM { chunk =>
          chunk.mapM(record => toRecord(shard.shardId(), record))
        }

        for {
          checkpointer <- leaseCoordinator.makeCheckpointer(shard)
          blocking     <- ZIO.environment[Blocking]
        } yield (
          shard.shardId(),
          shardStream.ensuringFirst(checkpointer.checkpoint.orDie.provide(blocking)),
          checkpointer
        )
      }
    }
  }

  val dummyCheckpointer = new Checkpointer {
    override def checkpoint: ZIO[zio.blocking.Blocking, Throwable, Unit] = ZIO.unit
    override def stage(r: Record[_]): zio.UIO[Unit]                      = ZIO.unit
  }

  implicit class ZioDebugExtensions[R, E, A](z: ZIO[R, E, A]) {
    def debug(label: String): ZIO[R, E, A] = (UIO(println(s"${label}")) *> z) <* UIO(println(s"${label} complete"))
  }

  def toConsumerRecord(record: KinesisRecord, shardId: String): ConsumerRecord =
    ConsumerRecord(
      record.sequenceNumber(),
      record.approximateArrivalTimestamp(),
      record.data(),
      record.partitionKey(),
      record.encryptionType(),
      shardId
    )

  val isThrottlingException: PartialFunction[Throwable, Unit] = {
    case _: KmsThrottlingException                 => ()
    case _: ProvisionedThroughputExceededException => ()
    case _: LimitExceededException                 => ()
  }

  def retryOnThrottledWithSchedule[R, A](schedule: Schedule[R, Throwable, A]): Schedule[R, Throwable, (Throwable, A)] =
    Schedule.doWhile[Throwable](e => isThrottlingException.lift(e).isDefined) && schedule

}
