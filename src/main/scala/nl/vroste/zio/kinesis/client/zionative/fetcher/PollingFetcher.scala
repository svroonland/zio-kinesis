package nl.vroste.zio.kinesis.client.zionative.fetcher
import nl.vroste.zio.kinesis.client.AdminClient.StreamDescription
import nl.vroste.zio.kinesis.client.Client
import nl.vroste.zio.kinesis.client.Util.throttledFunction
import nl.vroste.zio.kinesis.client.zionative.{ Consumer, DiagnosticEvent, FetchMode, Fetcher }
import software.amazon.awssdk.services.kinesis.model.Record
import zio.clock.Clock
import zio.duration._
import zio.stream.ZStream
import zio._
import zio.logging.log

import scala.jdk.CollectionConverters._
import nl.vroste.zio.kinesis.client.Util
import zio.logging.Logging
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException

object PollingFetcher {
  import Consumer.retryOnThrottledWithSchedule
  import Util._

  def make(
    streamDescription: StreamDescription,
    config: FetchMode.Polling,
    emitDiagnostic: DiagnosticEvent => UIO[Unit]
  ): ZManaged[Clock with Client with Logging, Throwable, Fetcher] =
    for {
      // Max 5 calls per second (globally)
      getShardIterator <- throttledFunction(5, 1.second)((Client.getShardIterator _).tupled)
      env              <- ZIO.environment[Client with Clock with Logging].toManaged_
    } yield Fetcher { (shardId, startingPosition) =>
      ZStream.unwrapManaged {
        for {
          // GetRecords can be called up to 5 times per second per shard
          getRecordsThrottled  <- throttledFunction(5, 1.second)((Client.getRecords _).tupled)
          initialShardIterator <- getShardIterator((streamDescription.streamName, shardId, startingPosition))
                                    .retry(retryOnThrottledWithSchedule(config.throttlingBackoff))
                                    .toManaged_
          shardIterator        <- Ref.make[Option[String]](Some(initialShardIterator)).toManaged_

          // Failure with None indicates that there's no next shard iterator and the shard has ended
          doPoll                          = for {
                     currentIterator <- shardIterator.get
                     result          <- currentIterator match {
                                 case Some(currentIterator) =>
                                   for {
                                     responseWithDuration <-
                                       getRecordsThrottled(
                                         (currentIterator, config.batchSize)
                                       ).retry(
                                           retryOnThrottledWithSchedule(
                                             config.throttlingBackoff
                                           )
                                         )
                                         .asSomeError
                                         .retry(
                                           Schedule.fixed(
                                             100.millis
                                           ) && Schedule.recurs(3)
                                         ) // There is a race condition in kinesalite, see https://github.com/mhart/kinesalite/issues/25
                                         .timed
                                     (duration, response)  = responseWithDuration
                                     records               = response.records.asScala.toList
                                     _                    <- shardIterator.set(Option(response.nextShardIterator))
                                     _                    <- emitDiagnostic(
                                            DiagnosticEvent.PollComplete(
                                              shardId,
                                              records.size,
                                              response
                                                .millisBehindLatest()
                                                .toLong
                                                .millis,
                                              duration
                                            )
                                          )
                                   } yield Chunk.fromIterable(records)
                                 case None                  => ZIO.fail(None)
                               }
                   } yield result

          doPollAndRetryOnExpiredIterator = doPoll.catchSome {
                                              case Some(e: ExpiredIteratorException) =>
                                                log.warn(s"Shard iterator expired: ${e}") *>
                                                  getShardIterator(
                                                    (streamDescription.streamName, shardId, startingPosition)
                                                  ).retry(retryOnThrottledWithSchedule(config.throttlingBackoff))
                                                    .asSomeError
                                                    .flatMap(it => shardIterator.set(Some(it))) *> doPoll
                                            }

        } yield ZStream
          .repeatEffectWith(doPollAndRetryOnExpiredIterator, Schedule.fixed(config.interval))
          .catchAll {
            case None    => ZStream.empty
            case Some(e) =>
              ZStream.unwrap(
                log.warn(s"Error in PollingFetcher for shard ${shardId}: ${e}").as(ZStream.fail(e))
              )
          }
          .flattenChunks
          .retry(
            config.throttlingBackoff.tapOutput {
              case (delay, retryNr) =>
                log.info(s"PollingFetcher will make make retry attempt nr ${retryNr} in ${delay.toMillis} millis")
            }
          )
          .provide(env)
      }.provide(env)
    }
}
