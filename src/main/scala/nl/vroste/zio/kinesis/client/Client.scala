package nl.vroste.zio.kinesis.client

import java.time.Instant
import java.util.concurrent.CompletableFuture

import nl.vroste.zio.kinesis.client.serde.{ Deserializer, Serializer }
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model._
import zio.clock.Clock
import zio.stream.ZStream
import zio.{ Has, Schedule, Task, ZIO, ZLayer, ZManaged }
import zio.Promise
import zio.Queue
import zio.duration._

import software.amazon.awssdk.http.SdkCancellationException
import software.amazon.awssdk.http.nio.netty.Http2Configuration
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.http.Protocol
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder
import zio.IO
import software.amazon.awssdk.core.SdkBytes

object Client {

  val live: ZLayer[Has[KinesisAsyncClient], Throwable, Client] =
    ZLayer.fromService[KinesisAsyncClient, Client.Service] {
      new ClientLive(_)
    }

  /**
   * Client for consumer and producer operations
   *
   * The interface is as close as possible to the natural ZIO variant of the KinesisAsyncClient interface,
   * with some noteable differences:
   * - Methods working with records (consuming or producing) make use of Serdes for (de)serialization
   * - Paginated APIs such as listShards are modeled as a Stream
   * - AWS SDK library method responses that only indicate success and do not contain any other
   *   data (besides SDK internals) are mapped to a ZIO of Unit
   * - AWS SDK library method responses that contain a single field are mapped to a ZIO of that field's type
   *
   */
  trait Service {

    /**
     * Registers a stream consumer for use during the lifetime of the managed resource
     *
     * If the consumer already exists, it will be reused
     *
     * @param streamARN    ARN of the stream to consume
     * @param consumerName Name of the consumer
     * @return Managed resource that unregisters the stream consumer after use
     */
    def createConsumer(streamARN: String, consumerName: String): ZManaged[Any, Throwable, Consumer]

    def describeStreamConsumer(streamARN: String, consumerName: String): ZIO[Any, Throwable, ConsumerDescription]

    /**
     * List all shards in a stream
     *
     * Handles paginated responses from the AWS API in a streaming manner
     *
     * @param chunkSize Number of results to fetch in one request (maxResults parameter).
     *                  Overwrites the maxResults in the request
     *
     * @return ZStream of shards in a stream
     */
    def listShards(
      streamName: String,
      streamCreationTimestamp: Option[Instant] = None,
      chunkSize: Int = 10000
    ): ZStream[Clock, Throwable, Shard]

    def getShardIterator(
      streamName: String,
      shardId: String,
      iteratorType: ShardIteratorType
    ): Task[String]

    /**
     * Creates a `ZStream` of the records in the given shard
     *
     * Records are deserialized to values of type `T`
     *
     * Subscriptions are valid for only 5 minutes and should be renewed by the caller with
     * an up to date starting position.
     *
     * @param consumerARN
     * @param shardID
     * @param startingPosition
     * @return Stream of SubscribeToShardEvents, each of which contain records.
     *         When exceptions occur in the subscription or the streaming, the stream will fail.
     */
    def subscribeToShard(
      consumerARN: String,
      shardID: String,
      startingPosition: ShardIteratorType
    ): ZStream[Any, Throwable, SubscribeToShardEvent]

    /**
     * @see [[createConsumer]] for automatic deregistration of the consumer
     */
    def registerStreamConsumer(
      streamARN: String,
      consumerName: String
    ): ZIO[Any, Throwable, Consumer]

    /**
     * @see [[createConsumer]] for automatic deregistration of the consumer
     */
    def deregisterStreamConsumer(consumerARN: String): Task[Unit]

    def getRecords(shardIterator: String, limit: Int): Task[GetRecordsResponse]

    def putRecord[R, T](
      streamName: String,
      serializer: Serializer[R, T],
      r: ProducerRecord[T]
    ): ZIO[R, Throwable, PutRecordResponse]

    def putRecords[R, T](
      streamName: String,
      serializer: Serializer[R, T],
      records: Iterable[ProducerRecord[T]]
    ): ZIO[R, Throwable, PutRecordsResponse]

    def putRecords(streamName: String, entries: List[PutRecordsRequestEntry]): Task[PutRecordsResponse]
  }

  // Accessor methods
  def listShards(
    streamName: String,
    streamCreationTimestamp: Option[Instant] = None,
    chunkSize: Int = 10000
  ): ZStream[Clock with Client, Throwable, Shard] =
    ZStream.unwrap {
      ZIO.service[Service].map(_.listShards(streamName, streamCreationTimestamp, chunkSize))
    }

  def getShardIterator(streamName: String, shardId: String, iteratorType: ShardIteratorType): ClientTask[String] =
    withClient(_.getShardIterator(streamName, shardId, iteratorType))

  def getRecords(shardIterator: String, limit: Int): ClientTask[GetRecordsResponse] =
    withClient(_.getRecords(shardIterator, limit))

  type ClientTask[+A] = ZIO[Client, Throwable, A]

  private def withClient[R, R1 >: R, E, E1 <: E, A, A1 <: A](
    f: Client.Service => ZIO[R1, E1, A1]
  ): ZIO[Client with R, E, A] =
    ZIO.service[Service].flatMap(f)

  case class ConsumerRecord(
    sequenceNumber: String,
    approximateArrivalTimestamp: Instant,
    data: SdkBytes,
    partitionKey: String,
    encryptionType: EncryptionType,
    shardID: String
  )

  case class ProducerRecord[T](partitionKey: String, data: T)

  sealed trait ShardIteratorType
  object ShardIteratorType {
    case object Latest                                     extends ShardIteratorType
    case object TrimHorizon                                extends ShardIteratorType
    case class AtSequenceNumber(sequenceNumber: String)    extends ShardIteratorType
    case class AfterSequenceNumber(sequenceNumber: String) extends ShardIteratorType
    case class AtTimestamp(timestamp: Instant)             extends ShardIteratorType
  }

  /**
   * Optimizes the HTTP client for parallel shard streaming
   *
   * @param builder
   * @param maxConcurrency Set this to something like the number of leases + a bit more
   * @param initialWindowSize
   * @param healthCheckPingPeriod
   */
  def adjustKinesisClientBuilder(
    builder: KinesisAsyncClientBuilder,
    maxConcurrency: Int = Integer.MAX_VALUE,
    initialWindowSize: Int = 10 * 1024 * 1024,
    healthCheckPingPeriod: Duration = 60.seconds
  ) =
    builder
      .httpClientBuilder(
        NettyNioAsyncHttpClient
          .builder()
          .maxConcurrency(maxConcurrency)
          .http2Configuration(
            Http2Configuration
              .builder()
              .initialWindowSize(initialWindowSize)
              .healthCheckPingPeriod(healthCheckPingPeriod.asJava)
              .build()
          )
          .protocol(Protocol.HTTP2)
      )

}

private object Util {
  def asZIO[T](f: => CompletableFuture[T]): Task[T] = ZIO.effect(f).flatMap(ZIO.fromCompletionStage(_))

  def paginatedRequest[R, E, A, Token](fetch: Option[Token] => ZIO[R, E, (A, Option[Token])])(
    throttling: Schedule[Clock, Any, Int] = Schedule.forever
  ): ZStream[Clock with R, E, A] =
    ZStream.fromEffect(fetch(None)).flatMap {
      case (results, nextTokenOpt) =>
        ZStream.succeed(results) ++ (nextTokenOpt match {
          case None            => ZStream.empty
          case Some(nextToken) =>
            ZStream.paginateM[R, E, A, Token](nextToken)(token => fetch(Some(token))).scheduleElements(throttling)
        })
    }

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
   * @return The original function with rate limiting applied, as a managed resource
   */
  def throttledFunction[R, I, E, A](units: Long, duration: Duration)(
    f: I => ZIO[R, E, A]
  ): ZManaged[Clock, Nothing, I => ZIO[R, E, A]] =
    for {
      requestsQueue <- Queue.unbounded[(IO[E, A], Promise[E, A])].toManaged_
      _             <- ZStream
             .fromQueueWithShutdown(requestsQueue)
             .throttleShape(units, duration, units)(_ => 1)
             .mapM { case (effect, promise) => promise.complete(effect) }
             .runDrain
             .forkManaged
    } yield (input: I) =>
      for {
        env     <- ZIO.environment[R]
        promise <- Promise.make[E, A]
        _       <- requestsQueue.offer((f(input).provide(env), promise))
        result  <- promise.await
      } yield result

}
