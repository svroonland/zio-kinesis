package nl.vroste.zio.kinesis.client

import java.nio.ByteBuffer
import java.util.UUID

import nl.vroste.zio.kinesis.client.Util.processWithSkipOnError
import nl.vroste.zio.kinesis.client.serde.Deserializer
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.kinesis.common.{ InitialPositionInStream, InitialPositionInStreamExtended }
import software.amazon.kinesis.exceptions.ShutdownException
import software.amazon.kinesis.processor.RecordProcessorCheckpointer
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration.{ Duration, _ }
import zio.logging.{ Logger, Logging }
import zio.stream.{ ZStream, ZTransducer }

/**
 * Offers a ZStream based interface to the Amazon Kinesis Client Library (KCL)
 *
 * Ensures proper resource shutdown and failure handling
 */
object DynamicConsumer {
  // For (some) backwards compatibility
  type Record[T] = nl.vroste.zio.kinesis.client.Record[T]

  val live: ZLayer[Logging with Has[KinesisAsyncClient] with Has[CloudWatchAsyncClient] with Has[
    DynamoDbAsyncClient
  ], Nothing, Has[Service]] =
    ZLayer.fromServices[Logger[
      String
    ], KinesisAsyncClient, CloudWatchAsyncClient, DynamoDbAsyncClient, DynamicConsumer.Service] {
      new DynamicConsumerLive(_, _, _, _)
    }

  def fake(
    shards: ZStream[Any, Throwable, (String, ZStream[Any, Throwable, ByteBuffer])],
    refCheckpointedList: Ref[Seq[Any]]
  ): ZLayer[Clock, Nothing, Has[Service]] =
    ZLayer.fromService[Clock.Service, DynamicConsumer.Service] { clock =>
      new DynamicConsumerFake(shards, refCheckpointedList, clock)
    }

  trait Service {

    /**
     * Create a ZStream of records on a Kinesis stream with substreams per shard
     *
     * Uses DynamoDB for lease coordination between different instances of consumers
     * with the same application name and for offset checkpointing.
     *
     * @param streamName Name of the Kinesis stream
     * @param applicationName Application name for coordinating shard leases
     * @param deserializer Deserializer for record values
     * @param requestShutdown Effect that when completed will trigger a graceful shutdown of the KCL
     *   and the streams.
     * @param initialPosition Position in stream to start at when there is no previous checkpoint
     *   for this application
     * @param isEnhancedFanOut Flag for setting retrieval config - defaults to `true`. If `false` polling config is set.
     * @param leaseTableName Optionally set the lease table name - defaults to None. If not specified the `applicationName` will be used.
     * @param workerIdentifier Identifier used for the worker in this application group. Used in logging
     *   and written to the lease table.
     * @param maxShardBufferSize The maximum number of records per shard to store in a queue before blocking
     *   the KCL record processor until records have been dequeued. Note that the stream returned from this
     *   method will have internal chunk buffers as well.
     * @tparam R ZIO environment type required by the `deserializer`
     * @tparam T Type of record values
     * @return A nested ZStream - the outer ZStream represents the collection of shards, and the inner ZStream represents the individual shard
     */
    def shardedStream[R, T](
      streamName: String,
      applicationName: String,
      deserializer: Deserializer[R, T],
      requestShutdown: UIO[Unit] = UIO.never,
      initialPosition: InitialPositionInStreamExtended =
        InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON),
      isEnhancedFanOut: Boolean = true,
      leaseTableName: Option[String] = None,
      workerIdentifier: String = UUID.randomUUID().toString,
      maxShardBufferSize: Int = 1024 // Prefer powers of 2
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
    isEnhancedFanOut: Boolean = true,
    leaseTableName: Option[String] = None,
    workerIdentifier: String = UUID.randomUUID().toString,
    maxShardBufferSize: Int = 1024 // Prefer powers of 2
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
            isEnhancedFanOut,
            leaseTableName,
            workerIdentifier,
            maxShardBufferSize
          )
        )
    )

  /**
   * Similar to `shardedStream` accessor but provides the `recordProcessor` callback function for processing records
   * and takes care of checkpointing. The other difference is that it returns a ZIO of unit rather than a ZStream.
   *
   * @param streamName Name of the Kinesis stream
   * @param applicationName Application name for coordinating shard leases
   * @param deserializer Deserializer for record values
   * @param requestShutdown Effect that when completed will trigger a graceful shutdown of the KCL
   *   and the streams.
   * @param initialPosition Position in stream to start at when there is no previous checkpoint
   *   for this application
   * @param isEnhancedFanOut Flag for setting retrieval config - defaults to `true`. If `false` polling config is set.
   * @param leaseTableName Optionally set the lease table name - defaults to None. If not specified the `applicationName` will be used.
   * @param workerIdentifier Identifier used for the worker in this application group. Used in logging
   *   and written to the lease table.
   * @param maxShardBufferSize The maximum number of records per shard to store in a queue before blocking
   *   the KCL record processor until records have been dequeued. Note that the stream returned from this
   *   method will have internal chunk buffers as well.
   * @param checkpointBatchSize Maximum number of records before checkpointing
   * @param checkpointDuration Maximum interval before checkpointing
   * @param recordProcessor A function for processing a `Record[T]`
   * @tparam R ZIO environment type required by the `deserializer` and the `recordProcessor`
   * @tparam T Type of record values
   * @return A ZIO that completes with Unit when record processing is stopped via requestShutdown or fails when the consumer stream fails
   */
  def consumeWith[R, RC, T](
    streamName: String,
    applicationName: String,
    deserializer: Deserializer[R, T],
    requestShutdown: UIO[Unit] = UIO.never,
    initialPosition: InitialPositionInStreamExtended =
      InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON),
    isEnhancedFanOut: Boolean = true,
    leaseTableName: Option[String] = None,
    workerIdentifier: String = UUID.randomUUID().toString,
    maxShardBufferSize: Int = 1024, // Prefer powers of 2
    checkpointBatchSize: Long = 200,
    checkpointDuration: Duration = 5.second
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
               isEnhancedFanOut,
               leaseTableName,
               workerIdentifier,
               maxShardBufferSize
             )
             .flatMapPar(Int.MaxValue) {
               case (_, shardStream, checkpointer) =>
                 ZStream.fromEffect(Ref.make(false)).flatMap { refSkip =>
                   shardStream
                     .tap(record =>
                       processWithSkipOnError(refSkip)(
                         recordProcessor(record) *> checkpointer.stage(record)
                       )
                     )
                     .via(
                       checkpointer
                         .checkpointBatched[Blocking with Logging with RC](
                           nr = checkpointBatchSize,
                           interval = checkpointDuration
                         )
                     )
                 }
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
    private[client] def peek: UIO[Option[Record[_]]]

    /**
     * Stages a record for checkpointing
     *
     * Checkpoints are not actually performed until `checkpoint` is called
     *
     * @param r Record to checkpoint
     * @return Effect that completes immediately
     */
    def stage(r: Record[_]): UIO[Unit]

    /**
     * Helper method that ensures that a checkpoint is staged when 'effect' completes
     * successfully, even when the fiber is interrupted. When 'effect' fails or is itself
     * interrupted, the checkpoint is not staged.
     *
     * @param effect Effect to execute
     * @param r Record to stage a checkpoint for
     * @return Effect that completes with the result of 'effect'
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
     * - [[software.amazon.kinesis.exceptions.ShutdownException]] when the lease for this shard has been lost, when
     *   another worker has stolen the lease (this can happen at any time).
     * - [[software.amazon.kinesis.exceptions.ThrottlingException]]
     *
     * See also [[RecordProcessorCheckpointer]]
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
     * Usage:
     *    shardStream.via(checkpointer.checkpointBatched(1000, 1.second))
     *
     * @param nr Maximum number of records before checkpointing
     * @param interval Maximum interval before checkpointing
     * @return Function that results in a ZStream that produces Unit values for successful checkpoints,
     *         fails with an exception when checkpointing fails or becomes an empty stream
     *         when the lease for this shard is lost, thereby ending the stream.
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

  object Checkpointer {
    private[client] def make(kclCheckpointer: RecordProcessorCheckpointer, logger: Logger[String]): UIO[Checkpointer] =
      for {
        latestStaged <- Ref.make[Option[Record[_]]](None)
      } yield new Checkpointer {
        override def stage(r: Record[_]): UIO[Unit] =
          latestStaged.set(Some(r))

        override def checkpoint: ZIO[Blocking, Throwable, Unit] =
          latestStaged.get.flatMap {
            case Some(record) =>
              logger.info(s"about to checkpoint: shardId=${record.shardId} partitionKey=${record.partitionKey}") *>
                zio.blocking.blocking {
                  Task(kclCheckpointer.checkpoint(record.sequenceNumber, record.subSequenceNumber.getOrElse(0L)))
                } *> latestStaged.update {
                case Some(r) if r == record => None
                case r                      => r // A newer record may have been staged by now
              }
            case None         => UIO.unit
          }

        override private[client] def peek: UIO[Option[Record[_]]] = latestStaged.get
      }
  }

}
