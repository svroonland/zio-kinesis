package nl.vroste.zio.kinesis.client.zionative.fetcher

import nl.vroste.zio.kinesis.client.AdminClient.StreamDescription
import nl.vroste.zio.kinesis.client.Client.ShardIteratorType
import nl.vroste.zio.kinesis.client.zionative.{ Consumer, DiagnosticEvent, FetchMode, Fetcher }
import nl.vroste.zio.kinesis.client.{ Client, Util }
import software.amazon.awssdk.services.kinesis.model.{ ConsumerStatus, Record, ResourceInUseException }
import zio._
import zio.clock.Clock
import zio.duration._
import zio.logging.{ log, Logging }
import zio.stream.ZStream

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

object EnhancedFanOutFetcher {
  import Util.ZStreamExtensions

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
  // TODO configure: deregister stream consumers after use
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
      subscribeThrottled <- Util.throttledFunctionN(config.maxSubscriptionsPerSecond.toLong, 1.second) {
                              (pos: ShardIteratorType, shardId: String) =>
                                ZIO.succeed(client.subscribeToShard(consumerARN, shardId, pos))
                            }
      // TODO we need to wait until the consumer becomes active, otherwise there will be no events in the stream
    } yield Fetcher { (shardId, startingPosition) =>
      ZStream.unwrap {
        for {
          currentPosition <- Ref.make[Option[ShardIteratorType]](Some(startingPosition))
          stream           = {
            def s: ZStream[Clock with Logging, Throwable, Record] =
              ZStream
                .fromEffect(
                  currentPosition.get
                    .flatMap(
                      _.map(pos => subscribeThrottled((pos, shardId))).getOrElse(ZIO.succeed(ZStream.empty))
                    )
                )
                .flatten
                .tap { e =>
                  currentPosition.set(
                    Option(e.continuationSequenceNumber()).map(ShardIteratorType.AfterSequenceNumber)
                  )
                }
                .tap { e =>
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
                .repeat(
                  Schedule.doUntilM(_ => currentPosition.get.map(_.isEmpty))
                ) // Shard subscriptions get canceled after 5 minutes, resulting in stream completion.
                .retry((Schedule.succeed(config.retryDelay) && Schedule.forever).tapOutput {
                  case (delay, retryNr) =>
                    log.info(s"PollingFetcher will make make retry attempt nr ${retryNr} in ${delay.toMillis} millis")
                })
                .catchSome {
                  case e if Consumer.isThrottlingException.isDefinedAt(e) =>
                    ZStream.unwrap(ZIO.sleep(config.retryDelay).tap(_ => UIO(println("Throttled!"))).as(s))
                  case NonFatal(e)                                        =>
                    ZStream.unwrap(
                      log
                        .warn(s"Error in EnhancedFanOutFetcher for shard ${shardId}, will retry. ${e}")
                        .tap(_ => ZIO.sleep(config.retryDelay))
                        .as(s)
                    )
                } // TODO this should be replaced with a ZStream#retry with a proper exponential backoff scheme

            s
          }

        } yield stream
      }.provide(env)
    }
}
