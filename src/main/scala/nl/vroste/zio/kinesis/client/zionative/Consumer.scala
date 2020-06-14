package nl.vroste.zio.kinesis.client.zionative

import nl.vroste.zio.kinesis.client.AdminClient.StreamDescription
import nl.vroste.zio.kinesis.client.Client.{ ConsumerRecord, ShardIteratorType }
import nl.vroste.zio.kinesis.client.DynamicConsumer.Record
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
import nl.vroste.zio.kinesis.client.zionative.dynamodb.DynamoDbLeaseCoordinator.ExtendedSequenceNumber
import nl.vroste.zio.kinesis.client.zionative.LeaseCoordinator.AcquiredLease
import zio.random.Random

case object ShardLeaseLost

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
  def makeCheckpointer(shard: Shard): Task[Checkpointer]

  def getCheckpointForShard(shard: Shard): UIO[Option[ExtendedSequenceNumber]]

  // TODO current shards should probably be a stream or ref or something
  def acquiredLeases: ZStream[Clock, Throwable, AcquiredLease]

  def releaseLease(shardId: String): Task[Unit]
}

object LeaseCoordinator {
  case class AcquiredLease(shardId: String, leaseLost: Promise[Nothing, Unit])
}

object Consumer {
  def shardedStream[R, T](
    streamName: String,
    applicationName: String,
    deserializer: Deserializer[R, T],
    fetchMode: FetchMode = FetchMode.Polling(),
    initialStartingPosition: ShardIteratorType = ShardIteratorType.TrimHorizon,
    workerId: String = "worker1"
  ): ZStream[
    Blocking with Clock with Random with Has[Client] with Has[AdminClient] with Has[DynamoDbAsyncClient],
    Throwable,
    (String, ZStream[R with Blocking with Clock, Throwable, Record[T]], Checkpointer)
  ] =
    ZStream.unwrap {
      for {
        client       <- ZIO.service[Client]
        adminClient  <- ZIO.service[AdminClient]
        dynamoClient <- ZIO.service[DynamoDbAsyncClient]
      } yield {

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

        def makeFetcher(streamDescription: StreamDescription): ZManaged[Clock, Throwable, Fetcher] =
          fetchMode match {
            case c: Polling     => PollingFetcher.make(client, streamDescription, c)
            case EnhancedFanOut => EnhancedFanOutFetcher.make(client, streamDescription, applicationName)
          }

        ZStream.unwrapManaged {
          for {
            streamDescription <- adminClient.describeStream(streamName).toManaged_
            fetcher           <- makeFetcher(streamDescription)
            currentShards     <- client.listShards(streamName).runCollect.map(_.map(l => (l.shardId(), l)).toMap).toManaged_
            leaseCoordinator  <- DynamoDbLeaseCoordinator.make(dynamoClient, applicationName, workerId, currentShards)
          } yield leaseCoordinator.acquiredLeases.collect {
            case AcquiredLease(shardId, leaseLost) if currentShards.contains(shardId) =>
              (currentShards.get(shardId).get, leaseLost)
          }.mapM {
            case (shard, leaseLost) =>
              for {
                checkpointer        <- leaseCoordinator.makeCheckpointer(shard)
                blocking            <- ZIO.environment[Blocking]
                startingPositionOpt <- leaseCoordinator.getCheckpointForShard(shard)
                startingPosition     = startingPositionOpt
                                     .map(s => ShardIteratorType.AfterSequenceNumber(s.sequenceNumber))
                                     .getOrElse(initialStartingPosition)
                _                   <- UIO(println(s"${shard.shardId} start at ${startingPosition}"))
                shardStream          = (fetcher
                                  .shardRecordStream(shard, startingPosition)
                                  .mapChunksM { chunk =>
                                    chunk.mapM(record => toRecord(shard.shardId(), record))
                                  }
                                  .map(Exit.succeed(_)) ++ ZStream.fromEffect(
                                  leaseLost.await.as(Exit.fail(None))
                                )).collectWhileSuccess
              } yield (
                shard.shardId(),
                shardStream.ensuringFirst(
                  checkpointer.checkpoint.ignore.provide(blocking) *>
                    leaseCoordinator.releaseLease(shard.shardId()).ignore
                ),
                checkpointer
              )
          }
        }
      }
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
