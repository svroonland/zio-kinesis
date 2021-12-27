package nl.vroste.zio.kinesis.client.dynamicconsumer

import io.github.vigoo.zioaws.cloudwatch.CloudWatch
import io.github.vigoo.zioaws.dynamodb.DynamoDb
import io.github.vigoo.zioaws.kinesis.Kinesis
import nl.vroste.zio.kinesis.client.dynamicconsumer.fake.DynamicConsumerFake
import nl.vroste.zio.kinesis.client.serde.Deserializer
import software.amazon.awssdk.services.kinesis.model.EncryptionType
import software.amazon.kinesis.common.{ InitialPositionInStream, InitialPositionInStreamExtended }
import software.amazon.kinesis.exceptions.ShutdownException
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration.{ durationInt, Duration }
import zio.logging.{ Logger, Logging }
import zio.stream.{ ZStream, ZTransducer }

import java.time.Instant
import java.util.UUID

/**
 * Offers a ZStream based interface to the Amazon Kinesis Client Library (KCL)
 *
 * Ensures proper resource shutdown and failure handling
 */
object DynamicConsumer {
  final case class Record[+T](
    shardId: String,
    sequenceNumber: String,
    approximateArrivalTimestamp: Instant,
    data: T,
    partitionKey: String,
    encryptionType: EncryptionType,
    subSequenceNumber: Option[Long],
    explicitHashKey: Option[String],
    aggregated: Boolean
  )

  val live: ZLayer[Logging with Kinesis with CloudWatch with DynamoDb, Nothing, DynamicConsumer] =
    ZLayer
      .fromServices[Logger[String], Kinesis.Service, CloudWatch.Service, DynamoDb.Service, DynamicConsumer.Service] {
        case (logger, kinesis, cloudwatch, dynamodb) =>
          new DynamicConsumerLive(logger, kinesis.api, cloudwatch.api, dynamodb.api)
      }

  /**
   * Implements a fake `DynamicConsumer` that also offers fake checkpointing functionality that can be tracked using the
   * `refCheckpointedList` parameter.
   * @param shards
   *   A ZStream that is a fake representation of a Kinesis shard. There are helper constructors to create these - see
   *   [[nl.vroste.zio.kinesis.client.dynamicconsumer.fake.DynamicConsumerFake.shardsFromIterables]] and
   *   [[nl.vroste.zio.kinesis.client.dynamicconsumer.fake.DynamicConsumerFake.shardsFromStreams]]
   * @param refCheckpointedList
   *   A Ref that will be used to store the checkpointed records
   * @return
   *   A ZLayer of the fake `DynamicConsumer` implementation
   */
  def fake(
    shards: ZStream[Any, Throwable, (String, ZStream[Any, Throwable, Chunk[Byte]])],
    refCheckpointedList: Ref[Seq[Record[Any]]]
  ): ZLayer[Clock, Nothing, Has[Service]] =
    ZLayer.fromService[Clock.Service, DynamicConsumer.Service] { clock =>
      new DynamicConsumerFake(shards, refCheckpointedList, clock)
    }

  /**
   * Overloaded version of above but without fake checkpointing functionality
   * @param shards
   *   A ZStream that is a fake representation of a Kinesis shard. There are helper constructors to create these - see
   *   [[nl.vroste.zio.kinesis.client.dynamicconsumer.fake.DynamicConsumerFake.shardsFromIterables]] and
   *   [[nl.vroste.zio.kinesis.client.dynamicconsumer.fake.DynamicConsumerFake.shardsFromStreams]]
   * @return
   *   A ZLayer of the fake `DynamicConsumer` implementation
   */
  def fake(
    shards: ZStream[Any, Throwable, (String, ZStream[Any, Throwable, Chunk[Byte]])]
  ): ZLayer[Clock, Nothing, Has[Service]] =
    ZLayer.fromServiceM[Clock.Service, Any, Nothing, DynamicConsumer.Service] { clock =>
      Ref.make[Seq[Record[Any]]](Seq.empty).map { refCheckpointedList =>
        new DynamicConsumerFake(shards, refCheckpointedList, clock)
      }
    }

  trait Service {

    /**
     * Create a ZStream of records on a Kinesis stream with substreams per shard
     *
     * Uses DynamoDB for lease coordination between different instances of consumers with the same application name and
     * for offset checkpointing.
     *
     * @param streamName
     *   Name of the Kinesis stream
     * @param applicationName
     *   Application name for coordinating shard leases
     * @param deserializer
     *   Deserializer for record values
     * @param requestShutdown
     *   Effect that when completed will trigger a graceful shutdown of the KCL and the streams.
     * @param initialPosition
     *   Position in stream to start at when there is no previous checkpoint for this application
     * @param leaseTableName
     *   Optionally set the lease table name - defaults to None. If not specified the `applicationName` will be used.
     * @param metricsNamespace
     *   CloudWatch metrics namespace
     * @param workerIdentifier
     *   Identifier used for the worker in this application group. Used in logging and written to the lease table.
     * @param maxShardBufferSize
     *   The maximum number of records per shard to store in a queue before blocking the KCL record processor until
     *   records have been dequeued. Note that the stream returned from this method will have internal chunk buffers as
     *   well.
     * @param configureKcl
     *   Make additional KCL Scheduler configurations
     * @tparam R
     *   ZIO environment type required by the `deserializer`
     * @tparam T
     *   Type of record values
     * @return
     *   A nested ZStream - the outer ZStream represents the collection of shards, and the inner ZStream represents the
     *   individual shard
     */
    def shardedStream[R, T](
      streamName: String,
      applicationName: String,
      deserializer: Deserializer[R, T],
      requestShutdown: UIO[Unit] = UIO.never,
      initialPosition: InitialPositionInStreamExtended =
        InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON),
      leaseTableName: Option[String] = None,
      metricsNamespace: Option[String] = None,
      workerIdentifier: String = UUID.randomUUID().toString,
      maxShardBufferSize: Int = 1024, // Prefer powers of 2
      configureKcl: SchedulerConfig => SchedulerConfig = identity
    ): ZStream[
      Blocking with R,
      Throwable,
      (String, ZStream[Blocking, Throwable, Record[T]], Checkpointer)
    ]
  }

  // Accessor
  def shardedStream[R, T](
    streamName: String,
    applicationName: String,
    deserializer: Deserializer[R, T],
    requestShutdown: UIO[Unit] = UIO.never,
    initialPosition: InitialPositionInStreamExtended =
      InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON),
    leaseTableName: Option[String] = None,
    metricsNamespace: Option[String] = None,
    workerIdentifier: String = UUID.randomUUID().toString,
    maxShardBufferSize: Int = 1024, // Prefer powers of 2
    configureKcl: SchedulerConfig => SchedulerConfig = identity
  ): ZStream[
    DynamicConsumer with Blocking with R,
    Throwable,
    (String, ZStream[Blocking, Throwable, Record[T]], Checkpointer)
  ] =
    ZStream.unwrap(
      ZIO
        .service[DynamicConsumer.Service]
        .map(
          _.shardedStream(
            streamName,
            applicationName,
            deserializer,
            requestShutdown,
            initialPosition,
            leaseTableName,
            metricsNamespace,
            workerIdentifier,
            maxShardBufferSize,
            configureKcl
          )
        )
    )

  /**
   * Similar to `shardedStream` accessor but provides the `recordProcessor` callback function for processing records and
   * takes care of checkpointing. The other difference is that it returns a ZIO of unit rather than a ZStream.
   *
   * @param streamName
   *   Name of the Kinesis stream
   * @param applicationName
   *   Application name for coordinating shard leases
   * @param deserializer
   *   Deserializer for record values
   * @param requestShutdown
   *   Effect that when completed will trigger a graceful shutdown of the KCL and the streams.
   * @param initialPosition
   *   Position in stream to start at when there is no previous checkpoint for this application
   * @param leaseTableName
   *   Optionally set the lease table name - defaults to None. If not specified the `applicationName` will be used.
   * @param metricsNamespace
   *   CloudWatch metrics namespace
   * @param workerIdentifier
   *   Identifier used for the worker in this application group. Used in logging and written to the lease table.
   * @param maxShardBufferSize
   *   The maximum number of records per shard to store in a queue before blocking the KCL record processor until
   *   records have been dequeued. Note that the stream returned from this method will have internal chunk buffers as
   *   well.
   * @param checkpointBatchSize
   *   Maximum number of records before checkpointing
   * @param checkpointDuration
   *   Maximum interval before checkpointing
   * @param recordProcessor
   *   A function for processing a `Record[T]`
   * @param configureKcl
   *   Make additional KCL Scheduler configurations
   * @tparam R
   *   ZIO environment type required by the `deserializer` and the `recordProcessor`
   * @tparam T
   *   Type of record values
   * @return
   *   A ZIO that completes with Unit when record processing is stopped via requestShutdown or fails when the consumer
   *   stream fails
   */
  def consumeWith[R, RC, T](
    streamName: String,
    applicationName: String,
    deserializer: Deserializer[R, T],
    requestShutdown: UIO[Unit] = UIO.never,
    initialPosition: InitialPositionInStreamExtended =
      InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON),
    leaseTableName: Option[String] = None,
    metricsNamespace: Option[String] = None,
    workerIdentifier: String = UUID.randomUUID().toString,
    maxShardBufferSize: Int = 1024, // Prefer powers of 2
    checkpointBatchSize: Long = 200,
    checkpointDuration: Duration = 5.minutes,
    configureKcl: SchedulerConfig => SchedulerConfig = identity
  )(
    recordProcessor: Record[T] => RIO[RC, Unit]
  ): ZIO[R with RC with Blocking with Logging with Clock with DynamicConsumer, Throwable, Unit] =
    for {
      consumer <- ZIO.service[DynamicConsumer.Service]
      _        <- consumer
                    .shardedStream(
                      streamName,
                      applicationName,
                      deserializer,
                      requestShutdown,
                      initialPosition,
                      leaseTableName,
                      metricsNamespace,
                      workerIdentifier,
                      maxShardBufferSize,
                      configureKcl
                    )
                    .flatMapPar(Int.MaxValue) { case (_, shardStream, checkpointer) =>
                      shardStream
                        .tap(record => recordProcessor(record) *> checkpointer.stage(record))
                        .via(
                          checkpointer
                            .checkpointBatched[Blocking with Logging with RC](
                              nr = checkpointBatchSize,
                              interval = checkpointDuration
                            )
                        )
                    }
                    .runDrain
    } yield ()

  /**
   * Staging area for checkpoints
   *
   * Guarantees that the last staged record is checkpointed upon stream shutdown / interruption
   */
  trait Checkpointer {

    /*
     * Helper method that returns current state - useful for debugging
     */
    private[client] def peek: UIO[Option[ExtendedSequenceNumber]]

    /**
     * Stages a record for checkpointing
     *
     * Checkpoints are not actually performed until `checkpoint` is called
     *
     * @param r
     *   Record to checkpoint
     * @return
     *   Effect that completes immediately
     */
    def stage(r: Record[_]): UIO[Unit]

    /**
     * Helper method that ensures that a checkpoint is staged when 'effect' completes successfully, even when the fiber
     * is interrupted. When 'effect' fails or is itself interrupted, the checkpoint is not staged.
     *
     * @param effect
     *   Effect to execute
     * @param r
     *   Record to stage a checkpoint for
     * @return
     *   Effect that completes with the result of 'effect'
     */
    def stageOnSuccess[R, E, A](effect: ZIO[R, E, A])(r: Record[_]): ZIO[R, E, A] =
      effect.onExit {
        case Exit.Success(_) => stage(r)
        case _               => UIO.unit
      }

    /**
     * Checkpoint the last staged checkpoint
     *
     * Exceptions you should be prepared to handle:
     *   - `software.amazon.kinesis.exceptions.ShutdownException` when the lease for this shard has been lost, when
     *     another worker has stolen the lease (this can happen at any time).
     *   - `software.amazon.kinesis.exceptions.ThrottlingException`
     *
     * See also `software.amazon.kinesis.processor.RecordProcessorCheckpointer`
     */
    def checkpoint: ZIO[Blocking, Throwable, Unit]

    /**
     * Immediately checkpoint this record
     */
    def checkpointNow(r: Record[_]): ZIO[Blocking, Throwable, Unit] =
      stage(r) *> checkpoint

    /**
     * Helper method to add batch checkpointing to a shard stream
     *
     * Usage: shardStream.via(checkpointer.checkpointBatched(1000, 1.second))
     *
     * @param nr
     *   Maximum number of records before checkpointing
     * @param interval
     *   Maximum interval before checkpointing
     * @return
     *   Function that results in a ZStream that produces Unit values for successful checkpoints, fails with an
     *   exception when checkpointing fails or becomes an empty stream when the lease for this shard is lost, thereby
     *   ending the stream.
     */
    def checkpointBatched[R](
      nr: Long,
      interval: Duration
    ): ZStream[R, Throwable, Any] => ZStream[R with Clock with Blocking, Throwable, Unit] =
      _.aggregateAsyncWithin(ZTransducer.foldUntil((), nr)((_, _) => ()), Schedule.fixed(interval))
        .tap(_ => checkpoint)
        .catchAll {
          case _: ShutdownException =>
            ZStream.empty
          case e                    =>
            ZStream.fail(e)
        }
  }
}
