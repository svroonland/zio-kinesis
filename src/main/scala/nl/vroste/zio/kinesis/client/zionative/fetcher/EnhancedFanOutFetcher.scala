package nl.vroste.zio.kinesis.client.zionative.fetcher

import nl.vroste.zio.kinesis.client.AdminClient.StreamDescription
import nl.vroste.zio.kinesis.client.Client.ShardIteratorType
import nl.vroste.zio.kinesis.client.zionative.Fetcher.EndOfShard
import nl.vroste.zio.kinesis.client.zionative.{ DiagnosticEvent, ExtendedSequenceNumber, FetchMode, Fetcher }
import nl.vroste.zio.kinesis.client.{ Client, Util }
import software.amazon.awssdk.services.kinesis.model.{ ChildShard, ConsumerStatus, ResourceInUseException }
import zio._
import zio.clock.Clock
import zio.duration._
import zio.logging.{ log, Logging }
import zio.stream.ZStream

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

object EnhancedFanOutFetcher {
  import FetchUtil.repeatWhileNotNone
  import Util.ZStreamExtensions

  def make(
    streamDescription: StreamDescription,
    workerId: String,
    config: FetchMode.EnhancedFanOut,
    emitDiagnostic: DiagnosticEvent => UIO[Unit]
  ): ZManaged[Clock with Client with Logging, Throwable, Fetcher] =
    for {
      client             <- ZIO.service[Client.Service].toManaged_
      env                <- ZIO.environment[Logging with Clock].toManaged_
      consumerARN        <- registerConsumerIfNotExists(streamDescription.streamARN, workerId).toManaged_
      subscribeThrottled <- Util.throttledFunctionN(config.maxSubscriptionsPerSecond, 1.second) {
                              (pos: ShardIteratorType, shardId: String) =>
                                ZIO.succeed(client.subscribeToShard(consumerARN, shardId, pos))
                            }
    } yield Fetcher { (shardId, startingPosition) =>
      ZStream.unwrap {
        for {
          currentPosition <- Ref.make[Option[ShardIteratorType]](Some(startingPosition)) // None means shard has ended
        } yield repeatWhileNotNone(currentPosition) { pos =>
          ZStream
            .unwrap(subscribeThrottled(pos, shardId))
            .tap { e =>
              currentPosition.set(
                Option(e.continuationSequenceNumber()).map(ShardIteratorType.AfterSequenceNumber)
              )
            }
            .tap { e =>
              emitDiagnostic(
                DiagnosticEvent
                  .SubscribeToShardEvent(shardId, e.records.size, e.millisBehindLatest().toLong.millis)
              )
            }
            .catchSome {
              case NonFatal(e) =>
                ZStream.unwrap(
                  log
                    .warn(s"Error in EnhancedFanOutFetcher for shard ${shardId}, will retry. ${e}")
                    .as(ZStream.fail(e))
                )
            }
            // Retry on connection loss, throttling exception, etc.
            // Note that retry has to be at this level, not the outermost ZStream because that reinitializes the start position
            .retry(config.retrySchedule)
        }.mapError(Left(_): Either[Throwable, EndOfShard])
          .flatMap { response =>
            if (response.hasChildShards) {
              val lastRecord     = response.records().asScala.last
              val lastSequenceNr = ExtendedSequenceNumber(lastRecord.sequenceNumber(), 0L)
              ZStream.succeed(response) ++ ZStream.fail(
                Right(EndOfShard(lastSequenceNr, response.childShards().asScala.toSeq))
              )
            } else
              ZStream.succeed(response)
          }
          .mapConcat(_.records.asScala)
      }.provide(env)
    }

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
}

object FetchUtil {
  def repeatWhileNotNone[Token, R, E, O](
    token: Ref[Option[Token]]
  )(stream: Token => ZStream[R, E, O]): ZStream[R, E, O] =
    ZStream.unwrap {
      token.get.map {
        case Some(t) => stream(t) ++ repeatWhileNotNone(token)(stream)
        case None    => ZStream.empty
      }
    }
}
