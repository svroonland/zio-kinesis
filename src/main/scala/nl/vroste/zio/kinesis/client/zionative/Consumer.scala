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
import zio.logging.log
import nl.vroste.zio.kinesis.client.zionative.LeaseCoordinator.AcquiredLease
import nl.vroste.zio.kinesis.client.zionative.fetcher.{ EnhancedFanOutFetcher, PollingFetcher }
import zio.random.Random
import zio.logging.Logging

case class ExtendedSequenceNumber(sequenceNumber: String, subSequenceNumber: Long)

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
  def makeCheckpointer(shard: Shard): ZIO[Clock with Logging, Throwable, Checkpointer]

  def getCheckpointForShard(shard: Shard): UIO[Option[ExtendedSequenceNumber]]

  // TODO current shards should probably be a stream or ref or something
  def acquiredLeases: ZStream[Clock, Throwable, AcquiredLease]

  def releaseLease(shardId: String): ZIO[Logging, Throwable, Unit]
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
    workerId: String = "worker1",
    emitDiagnostic: DiagnosticEvent => UIO[Unit] = _ => UIO.unit
  ): ZStream[
    Blocking with Clock with Random with Has[Client] with Has[AdminClient] with Has[DynamoDbAsyncClient] with Logging,
    Throwable,
    (String, ZStream[R with Blocking with Clock with Logging, Throwable, Record[T]], Checkpointer)
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

    def makeFetcher(streamDescription: StreamDescription): ZManaged[Clock with Has[Client], Throwable, Fetcher] =
      fetchMode match {
        case c: Polling     => PollingFetcher.make(streamDescription, c, emitDiagnostic)
        case EnhancedFanOut => EnhancedFanOutFetcher.make(streamDescription, applicationName)
      }

    ZStream.unwrapManaged {
      ZManaged
        .mapParN(
          ZManaged.unwrap(AdminClient.describeStream(streamName).map(makeFetcher)),
          for {
            currentShards    <- Client.listShards(streamName).runCollect.map(_.map(l => (l.shardId(), l)).toMap).toManaged_
            _                <- ZManaged.die(new Exception("No shards in stream!")).when(currentShards.isEmpty)
            leaseCoordinator <- DynamoDbLeaseCoordinator.make(applicationName, workerId, currentShards, emitDiagnostic)
          } yield (currentShards, leaseCoordinator)
        )(_ -> _)
        .map {
          case (fetcher, (currentShards, leaseCoordinator)) =>
            leaseCoordinator.acquiredLeases.collect {
              case AcquiredLease(shardId, leaseLost) if currentShards.contains(shardId) =>
                (currentShards.get(shardId).get, leaseLost)
            }.mapMPar(10) { // TODO config var: max shard starts or something
              case (shard, leaseLost) =>
                for {
                  checkpointer        <- leaseCoordinator.makeCheckpointer(shard)
                  blocking            <- ZIO.environment[Blocking]
                  startingPositionOpt <- leaseCoordinator.getCheckpointForShard(shard)
                  startingPosition     = startingPositionOpt
                                       .map(s => ShardIteratorType.AfterSequenceNumber(s.sequenceNumber))
                                       .getOrElse(initialStartingPosition)
                  stop                 = ZStream.fromEffect(leaseLost.await).as(Exit.fail(None))
                  shardStream          = (stop merge fetcher
                                    .shardRecordStream(shard, startingPosition)
                                    .mapChunksM { chunk => // mapM is slow
                                      chunk.mapM(record => toRecord(shard.shardId(), record))
                                    }
                                    .map(Exit.succeed(_))).collectWhileSuccess
                } yield (
                  shard.shardId(),
                  shardStream.ensuringFirst {
                    checkpointer.checkpointAndRelease.catchAll {
                      case Left(e)               => ZIO.fail(e)
                      case Right(ShardLeaseLost) => ZIO.unit // This is fine during shutdown
                    }.orDie
                      .provide(blocking)
                  },
                  checkpointer
                )
            }
        }
    }
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
