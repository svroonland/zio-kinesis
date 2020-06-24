package nl.vroste.zio.kinesis.client.zionative.fetcher

import nl.vroste.zio.kinesis.client.AdminClient.StreamDescription
import nl.vroste.zio.kinesis.client.Client
import nl.vroste.zio.kinesis.client.Client.ShardIteratorType
import nl.vroste.zio.kinesis.client.zionative.{ Consumer, Fetcher }
import zio.{ Ref, Schedule, ZIO, ZManaged }
import zio.clock.Clock
import zio.stream.ZStream
import zio.UIO
import zio.duration._
import zio.Has
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException
import software.amazon.awssdk.services.kinesis.model.ConsumerStatus
import scala.jdk.CollectionConverters._
import nl.vroste.zio.kinesis.client.Client.ConsumerRecord
import nl.vroste.zio.kinesis.client.zionative.DiagnosticEvent
import io.netty.handler.timeout.ReadTimeoutException
import zio.logging.log
import zio.logging.Logging
import java.io.IOException

object EnhancedFanOutFetcher {
  private def registerConsumerIfNotExists(streamARN: String, consumerName: String) =
    ZIO.service[Client].flatMap(_.registerStreamConsumer(streamARN, consumerName)).map(_.consumerARN()).catchSome {
      case e: ResourceInUseException =>
        // Consumer already exists, retrieve it
        ZIO
          .service[Client]
          .flatMap(_.describeStreamConsumer(streamARN, consumerName))
          .filterOrElse(_.consumerStatus() != ConsumerStatus.DELETING)(_ => ZIO.fail(e))
          .map(_.consumerARN())
    }

  // TODO doesn't really need to be managed anymore
  def make(
    streamDescription: StreamDescription,
    workerId: String,
    emitDiagnostic: DiagnosticEvent => UIO[Unit]
  ): ZManaged[Clock with Has[Client] with Logging, Throwable, Fetcher] =
    for {
      client      <- ZIO.service[Client].toManaged_
      env         <- ZIO.environment[Logging with Clock].toManaged_
      _            = println("Creating consumer")
      consumerARN <- registerConsumerIfNotExists(streamDescription.streamARN, workerId).toManaged_
    } yield Fetcher { (shard, startingPosition) =>
      ZStream.unwrap {
        for {
          currentPosition <- Ref.make[ShardIteratorType](startingPosition)
          stream           = ZStream
                     .fromEffect(currentPosition.get)
                     .flatMap { pos =>
                       client
                         .subscribeToShard(
                           consumerARN,
                           shard.shardId(),
                           pos
                         )
                     }
                     .tap { e =>
                       currentPosition.set(ShardIteratorType.AfterSequenceNumber(e.continuationSequenceNumber)) *>
                         emitDiagnostic(
                           DiagnosticEvent
                             .SubscribeToShardEvent(
                               shard.shardId(),
                               e.records.size,
                               e.millisBehindLatest().toLong.millis
                             )
                         )
                     }
                     .mapConcat(_.records.asScala)
                     .map { record =>
                       ConsumerRecord(
                         record.sequenceNumber(),
                         record.approximateArrivalTimestamp(),
                         record.data(),
                         record.partitionKey(),
                         record.encryptionType(),
                         shard.shardId()
                       )
                     }
                     .repeat(Schedule.forever) // Shard subscriptions get canceled after 5 minutes

        } yield stream.catchSome {
          case e if Consumer.isThrottlingException.isDefinedAt(e) =>
            ZStream.unwrap(ZIO.sleep(1.second).tap(_ => UIO(println("Throttled!"))).as(stream))
          case e @ (_: ReadTimeoutException | _: IOException)     =>
            ZStream.unwrap(
              log.warn(s"SubscribeToShard IO error for shard ${shard.shardId()}, will retry: ${e}").as(stream)
            )

        } // TODO this should be replaced with a ZStream#retry with a proper exponential backoff scheme
      }.provide(env)
    }
}
