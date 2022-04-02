package nl.vroste.zio.kinesis.client.zionative.fetcher

import zio.aws.kinesis.Kinesis
import zio.aws.kinesis.model._
import nl.vroste.zio.kinesis.client.Util
import nl.vroste.zio.kinesis.client.zionative.Consumer.childShardToShard
import nl.vroste.zio.kinesis.client.zionative.Fetcher.EndOfShard
import nl.vroste.zio.kinesis.client.zionative.{ DiagnosticEvent, FetchMode, Fetcher }
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException
import zio._
import zio.stream.ZStream

import scala.util.control.NonFatal
import zio.Clock
import zio.aws.kinesis.model.primitives.{ ConsumerName, ShardId, StreamARN }

object EnhancedFanOutFetcher {
  import FetchUtil.repeatWhileNotNone

  def make(
    streamDescription: StreamDescription.ReadOnly,
    workerId: String,
    config: FetchMode.EnhancedFanOut,
    emitDiagnostic: DiagnosticEvent => UIO[Unit]
  ): ZIO[Scope with Clock with Kinesis, Throwable, Fetcher] =
    for {
      env                <- ZIO.environment[Clock with Kinesis]
      consumerARN        <- registerConsumerIfNotExists(streamDescription.streamARN, workerId)
      subscribeThrottled <- Util.throttledFunctionN(config.maxSubscriptionsPerSecond, 1.second) {
                              (pos: StartingPosition, shardId: String) =>
                                ZIO.succeed(
                                  Kinesis
                                    .subscribeToShard(
                                      SubscribeToShardRequest(consumerARN, ShardId(shardId), pos)
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
                Option(e.continuationSequenceNumber).map(nr =>
                  StartingPosition(ShardIteratorType.AFTER_SEQUENCE_NUMBER, Some(nr))
                )
              )
            }
            .tap { e =>
              emitDiagnostic(
                DiagnosticEvent
                  .SubscribeToShardEvent(shardId, e.records.size, e.millisBehindLatest.millis)
              )
            }
            .catchSome { case NonFatal(e) =>
              ZStream.unwrap(
                ZIO
                  .logWarning(s"Error in EnhancedFanOutFetcher for shard ${shardId}, will retry. ${e}")
                  .as(ZStream.fail(e))
              )
            }
            // Retry on connection loss, throttling exception, etc.
            // Note that retry has to be at this level, not the outermost ZStream because that reinitializes the start position
            .retry(config.retrySchedule)
        }.mapError(Left(_): Either[Throwable, EndOfShard])
          .flatMap { response =>
            if (response.childShards.toList.flatten.nonEmpty)
              ZStream.succeed(response) ++ ZStream.fail(
                Right(EndOfShard(response.childShards.toList.flatten.map(childShardToShard)))
              )
            else
              ZStream.succeed(response)
          }
          .mapConcat(_.records)
      }.provideEnvironment(env)
    }

  private def registerConsumerIfNotExists(streamARN: String, consumerName: String) =
    Kinesis
      .registerStreamConsumer(RegisterStreamConsumerRequest(StreamARN(streamARN), ConsumerName(consumerName)))
      .mapError(_.toThrowable)
      .map(_.consumer.consumerARN)
      .catchSome { case e: ResourceInUseException =>
        // Consumer already exists, retrieve it
        Kinesis
          .describeStreamConsumer(
            DescribeStreamConsumerRequest(
              streamARN = Some(StreamARN(streamARN)),
              consumerName = Some(ConsumerName(consumerName))
            )
          )
          .mapError(_.toThrowable)
          .map(_.consumerDescription)
          .filterOrElseWith(_.consumerStatus != ConsumerStatus.DELETING)(_ => ZIO.fail(e))
          .map(_.consumerARN)
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
