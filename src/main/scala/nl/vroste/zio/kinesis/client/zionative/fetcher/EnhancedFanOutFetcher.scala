package nl.vroste.zio.kinesis.client.zionative.fetcher

import io.github.vigoo.zioaws.kinesis
import io.github.vigoo.zioaws.kinesis.Kinesis
import io.github.vigoo.zioaws.kinesis.model._
import nl.vroste.zio.kinesis.client.Util
import nl.vroste.zio.kinesis.client.zionative.{ DiagnosticEvent, FetchMode, Fetcher }
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException
import zio._
import zio.clock.Clock
import zio.duration._
import zio.logging.{ log, Logging }
import zio.stream.ZStream

import scala.util.control.NonFatal

object EnhancedFanOutFetcher {
  import FetchUtil.repeatWhileNotNone
  import Util.ZStreamExtensions

  def make(
    streamDescription: StreamDescription,
    workerId: String,
    config: FetchMode.EnhancedFanOut,
    emitDiagnostic: DiagnosticEvent => UIO[Unit]
  ): ZManaged[Clock with Kinesis with Logging, Throwable, Fetcher] =
    for {
      client             <- ZIO.service[Kinesis.Service].toManaged_
      env                <- ZIO.environment[Logging with Clock].toManaged_
      consumerARN        <- registerConsumerIfNotExists(streamDescription.streamARN, workerId).toManaged_
      subscribeThrottled <- Util.throttledFunctionN(config.maxSubscriptionsPerSecond, 1.second) {
                              (pos: StartingPosition, shardId: String) =>
                                ZIO.succeed(
                                  client
                                    .subscribeToShard(
                                      SubscribeToShardRequest(consumerARN, shardId, pos)
                                    )
                                    .mapError(_.toThrowable)
                                )
                            }
    } yield Fetcher { (shardId, startingPosition) =>
      ZStream.unwrap {
        for {
          currentPosition <- Ref.make[Option[StartingPosition]](Some(startingPosition)) // None means shard has ended
        } yield repeatWhileNotNone(currentPosition) { pos =>
          ZStream
            .unwrap(subscribeThrottled(pos, shardId))
            .tap { e =>
              currentPosition.set(
                Option(e.continuationSequenceNumberValue).map(nr =>
                  StartingPosition(ShardIteratorType.AFTER_SEQUENCE_NUMBER, Some(nr))
                )
              )
            }
            .tap { e =>
              emitDiagnostic(
                DiagnosticEvent
                  .SubscribeToShardEvent(shardId, e.recordsValue.size, e.millisBehindLatestValue.millis)
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
        }.mapConcat(_.recordsValue.map(_.editable))
      }.provide(env)
    }

  private def registerConsumerIfNotExists(streamARN: String, consumerName: String) =
    kinesis
      .registerStreamConsumer(RegisterStreamConsumerRequest(streamARN, consumerName))
      .mapError(_.toThrowable)
      .map(_.consumerValue.consumerARNValue)
      .catchSome {
        case e: ResourceInUseException =>
          // Consumer already exists, retrieve it
          kinesis
            .describeStreamConsumer(
              DescribeStreamConsumerRequest(streamARN = Some(streamARN), consumerName = Some(consumerName))
            )
            .mapError(_.toThrowable)
            .map(_.consumerDescriptionValue)
            .filterOrElse(_.consumerStatusValue != ConsumerStatus.DELETING)(_ => ZIO.fail(e))
            .map(_.consumerARNValue)
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
