package nl.vroste.zio.kinesis.client.zionative.fetcher

import java.io.IOException

import io.netty.handler.timeout.ReadTimeoutException
import nl.vroste.zio.kinesis.client.AdminClient.StreamDescription
import nl.vroste.zio.kinesis.client.{ Client, Util }
import nl.vroste.zio.kinesis.client.Client.ShardIteratorType
import nl.vroste.zio.kinesis.client.zionative.{ Consumer, DiagnosticEvent, FetchMode, Fetcher }
import software.amazon.awssdk.services.kinesis.model.{ ConsumerStatus, ResourceInUseException }
import zio.clock.Clock
import zio.duration._
import zio.logging.{ log, Logging }
import zio.stream.ZStream
import zio._

import scala.jdk.CollectionConverters._

object EnhancedFanOutFetcher {
  private def registerConsumerIfNotExists(streamARN: String, consumerName: String) =
    ZIO
      .service[Client.Service]
      .flatMap(_.registerStreamConsumer(streamARN, consumerName))
      .map(_.consumerARN())
      .catchSome {
        case e: ResourceInUseException =>
          // Consumer already exists, retrieve it
          ZIO
            .service[Client.Service]
            .flatMap(_.describeStreamConsumer(streamARN, consumerName))
            .filterOrElse(_.consumerStatus() != ConsumerStatus.DELETING)(_ => ZIO.fail(e))
            .map(_.consumerARN())
      }

  // TODO doesn't really need to be managed anymore
  // TODO configure: deregsiter stream consumers after use
  def make(
    streamDescription: StreamDescription,
    workerId: String,
    config: FetchMode.EnhancedFanOut,
    emitDiagnostic: DiagnosticEvent => UIO[Unit]
  ): ZManaged[Clock with Client with Logging, Throwable, Fetcher] =
    for {
      client             <- ZIO.service[Client.Service].toManaged_
      env                <- ZIO.environment[Logging with Clock].toManaged_
      _                   = println("Creating consumer")
      consumerARN        <- registerConsumerIfNotExists(streamDescription.streamARN, workerId).toManaged_
      subscribeThrottled <- Util.throttledFunctionN(config.maxSubscriptionsPerSecond, 1.second) {
                              (pos: ShardIteratorType, shardId: String) =>
                                ZIO.succeed(client.subscribeToShard(consumerARN, shardId, pos))
                            }
      // TODO we need to wait until the consumer becomes active, otherwise there will be no events in the stream
    } yield Fetcher { (shardId, startingPosition) =>
      ZStream.unwrap {
        for {
          currentPosition <- Ref.make[ShardIteratorType](startingPosition)
          stream           = ZStream
                     .fromEffect(currentPosition.get.flatMap(subscribeThrottled(_, shardId)))
                     .flatten
                     .tap { e =>
                       currentPosition.set(ShardIteratorType.AfterSequenceNumber(e.continuationSequenceNumber)) *>
                         emitDiagnostic(
                           DiagnosticEvent
                             .SubscribeToShardEvent(
                               shardId,
                               e.records.size,
                               e.millisBehindLatest().toLong.millis
                             )
                         )
                     }
                     .mapConcat(_.records.asScala)
                     .repeat(Schedule.forever) // Shard subscriptions get canceled after 5 minutes

        } yield stream.catchSome {
          case e if Consumer.isThrottlingException.isDefinedAt(e) =>
            ZStream.unwrap(ZIO.sleep(1.second).tap(_ => UIO(println("Throttled!"))).as(stream))
          case e @ (_: ReadTimeoutException | _: IOException)     =>
            ZStream.unwrap(
              log.warn(s"SubscribeToShard IO error for shard ${shardId}, will retry: ${e}").as(stream)
            )

        } // TODO this should be replaced with a ZStream#retry with a proper exponential backoff scheme
      }.provide(env)
    }
}
