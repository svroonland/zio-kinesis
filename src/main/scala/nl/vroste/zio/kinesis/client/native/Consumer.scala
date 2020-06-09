package nl.vroste.zio.kinesis.client.native

import nl.vroste.zio.kinesis.client.Client
import nl.vroste.zio.kinesis.client.AdminClient
import zio.stream.ZStream
import nl.vroste.zio.kinesis.client.Client.ShardIteratorType
import nl.vroste.zio.kinesis.client.DynamicConsumer.Record
import nl.vroste.zio.kinesis.client.DynamicConsumer.Checkpointer
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.serde.Deserializer
import nl.vroste.zio.kinesis.client.Client.ConsumerRecord
import zio.ZIO
import zio.clock.Clock
import zio.Schedule
import zio.Ref
import software.amazon.awssdk.services.kinesis.model.Shard
import zio.duration._
import zio.UIO

import scala.jdk.CollectionConverters._
import software.amazon.awssdk.services.kinesis.model.{ Record => KinesisRecord }
import zio.Chunk
import nl.vroste.zio.kinesis.client.native.FetchMode.Polling
import nl.vroste.zio.kinesis.client.native.FetchMode.EnhancedFanOut
import zio.ZManaged
import nl.vroste.zio.kinesis.client.AdminClient.StreamDescription
import software.amazon.awssdk.services.kinesis.model.KmsThrottlingException
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException
import software.amazon.awssdk.services.kinesis.model.LimitExceededException
import zio.Queue
import zio.Promise
import zio.IO
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse

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
  def fetch(shard: Shard, startingPosition: ShardIteratorType): ZStream[Clock, Throwable, ConsumerRecord]

  def shardStreams[R](
    shards: ZStream[R, Throwable, (Shard, ShardIteratorType)]
  ): ZStream[R, Throwable, (Shard, ZStream[Clock, Throwable, ConsumerRecord])] =
    shards.map { case (shard, startingPosition) => (shard, fetch(shard, startingPosition)) }
}

private object EnhancedFanOutFetcher {
  import Consumer.retryOnThrottledWithSchedule

  def shardStream(
    client: Client,
    consumer: software.amazon.awssdk.services.kinesis.model.Consumer,
    shard: Shard,
    startingPosition: ShardIteratorType
  ): ZStream[Clock, Throwable, ConsumerRecord] =
    ZStream.unwrap {
      for {
        currentPosition <- Ref.make[ShardIteratorType](startingPosition)
        val stream       = ZStream
                       .fromEffect(currentPosition.get)
                       .flatMap { pos =>
                         client
                           .subscribeToShard(
                             consumer.consumerARN(),
                             shard.shardId(),
                             pos
                           )
                       }
                       .tap(r => currentPosition.set(ShardIteratorType.AfterSequenceNumber(r.sequenceNumber)))
                       .repeat(Schedule.forever) // Shard subscriptions get canceled after 5 minutes

      } yield stream.catchSome(
        Consumer.isThrottlingException.andThen(_ => ZStream.unwrap(ZIO.sleep(1.second).as(stream)))
      ) // TODO this should be replaced with a ZStream#retry with a proper exponential backoff scheme
    }
}

private object PollingFetcher {
  import Consumer.retryOnThrottledWithSchedule

  def make(
    client: Client,
    streamDescription: StreamDescription,
    config: FetchMode.Polling
  ): ZManaged[Clock, Throwable, Fetcher] =
    for {
      // Max 5 calls per second (globally)
      getShardIterator <-
        Util.throttledFunction(5, 1.second, (client.getShardIterator _).tupled).map(Function.untupled(_))
    } yield new Fetcher {
      override def fetch(
        shard: Shard,
        startingPosition: ShardIteratorType
      ): ZStream[zio.clock.Clock, Throwable, ConsumerRecord] =
        ZStream.unwrapManaged {
          for {
            // Get records can be called up to 5 times per second per shard
            getRecordsThrottled  <-
              Util.throttledFunction(5, 1.second, Function.tupled(client.getRecords _)).map(Function.untupled(_))
            initialShardIterator <-
              getShardIterator(streamDescription.streamName, shard.shardId(), startingPosition).toManaged_
            delayRef             <- Ref.make[Boolean](false).toManaged_
            shardIterator        <- Ref.make[String](initialShardIterator).toManaged_
          } yield ZStream.repeatEffectChunkOption {
            for {
              _               <- (
                       // UIO(println("s${shard.shardId()}: delaying poll")) *>
                       ZIO.sleep(config.delay)
                   ).whenM(delayRef.get)
              currentIterator <- shardIterator.get
              response        <- getRecordsThrottled(currentIterator, config.batchSize)
                            .retry(retryOnThrottledWithSchedule(config.backoff))
                            .asSomeError
              records          = response.records.asScala.toList
              _                = println(s"${shard.shardId()}: Got ${records.size} records")
              _               <- delayRef.set(records.isEmpty)
              _               <- Option(response.nextShardIterator).map(shardIterator.set).getOrElse(ZIO.fail(None))
            } yield Chunk.fromIterable(records.map(Consumer.toConsumerRecord(_, shard.shardId())))
          }
        }
    }
}

object Consumer {
  def shardedStream[R, T](
    client: Client,
    adminClient: AdminClient,
    streamName: String,
    applicationName: String,
    deserializer: Deserializer[R, T],
    fetchMode: FetchMode = FetchMode.Polling()
  ): ZStream[Clock, Throwable, (String, ZStream[R with Clock, Throwable, Record[T]], Checkpointer)] = {

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

    val shardRefreshInterval = 1.minute

    val currentShards: ZStream[Clock, Throwable, Shard] = client.listShards(streamName)

    def makeFetcher(streamDescription: StreamDescription): ZManaged[Clock, Throwable, Fetcher] =
      fetchMode match {
        case c: Polling     =>
          PollingFetcher.make(client, streamDescription, c)
        case EnhancedFanOut =>
          client
            .createConsumer(streamDescription.streamARN, applicationName)
            .map(consumer =>
              (shard, startingPosition) => EnhancedFanOutFetcher.shardStream(client, consumer, shard, startingPosition)
            )

      }

    ZStream.unwrapManaged {
      for {
        streamDescription <- adminClient.describeStream(streamName).debug("desribeStream").toManaged_
        fetcher           <- makeFetcher(streamDescription)
      } yield fetcher.shardStreams(currentShards.map(_ -> ShardIteratorType.TrimHorizon)).map {
        case (shard, shardStream) =>
          val deserializedStream = shardStream.mapChunksM { chunk =>
            chunk.mapM { case record => toRecord(shard.shardId(), record) }
          }

          (shard.shardId(), deserializedStream, dummyCheckpointer)
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
    Schedule.doWhile[Throwable](e => isThrottlingException.lift(e).map(_ => true).getOrElse(false)) && schedule

}

object Util {
  // TODO add jitter
  def exponentialBackoff(
    min: Duration,
    max: Duration,
    factor: Double = 2.0,
    maxRecurs: Option[Int] = None
  ): Schedule[Clock, Throwable, Any] =
    (Schedule.exponential(min).whileOutput(_ <= max) andThen Schedule.fixed(max)) &&
      maxRecurs.map(Schedule.recurs).getOrElse(Schedule.forever)

  /**
   * Executes calls through a token bucket stream, ensuring a maximum rate of calls
   *
   * Allows for bursting
   *
    * @param units Maximum number of calls per duration
   * @param duration Duration for nr of tokens
   * @return A function that, given an effect, returns an effects that completes with the result of the given effect,
   *   but possibly delayed because of throttling.
   */
  def throttler[R, E, A](units: Long, duration: Duration): ZManaged[Clock, Nothing, ZIO[R, E, A] => ZIO[R, E, A]] =
    for {
      requestsQueue <- Queue.unbounded[(IO[E, A], Promise[E, A])].toManaged_
      stream        <- ZStream
                  .fromQueueWithShutdown(requestsQueue)
                  .throttleShape(units, duration, units)(_ => 1)
                  .mapM { case (effect, promise) => promise.completeWith(effect) }
                  .runDrain
                  .forkManaged
    } yield effect =>
      for {
        env     <- ZIO.environment[R]
        promise <- Promise.make[E, A]
        _       <- requestsQueue.offer((effect.provide(env), promise))
        result  <- promise.await
      } yield result

  def throttledFunction[R, I, E, A](
    units: Long,
    duration: Duration,
    f: I => ZIO[R, E, A]
  ): ZManaged[Clock, Nothing, I => ZIO[R, E, A]] = throttler[R, E, A](units, duration).map(ff => arg => ff(f(arg)))

}
