package nl.vroste.zio.kinesis.client

import java.time.Instant
import java.util.UUID

import nl.vroste.zio.kinesis.client.serde.Deserializer
import software.amazon.awssdk.services.kinesis.model.EncryptionType
import software.amazon.kinesis.common.{ InitialPositionInStream, InitialPositionInStreamExtended }
import software.amazon.kinesis.processor.RecordProcessorCheckpointer
import zio.blocking.Blocking
import zio.stream.ZStream
import zio._

/**
 * Offers a ZStream based interface to the Amazon Kinesis Client Library (KCL)
 *
 * Ensures proper resource shutdown and failure handling
 */
object DynamicConsumer {

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
     */
    def shardedStream[R, T](
      streamName: String,
      applicationName: String,
      deserializer: Deserializer[R, T], // TODO make Deserializer a ZLayer too
      requestShutdown: UIO[Unit] = UIO.never,
      initialPosition: InitialPositionInStreamExtended =
        InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON),
      isEnhancedFanOut: Boolean = true,
      leaseTableName: Option[String] = None,
      workerIdentifier: String = UUID.randomUUID().toString,
      maxShardBufferSize: Int = 1024    // Prefer powers of 2
    ): ZStream[
      Blocking with R,
      Throwable,
      (String, ZStream[Blocking, Throwable, Record[T]], Checkpointer)
    ]
  }

  case class Record[T](
    shardId: String,
    sequenceNumber: String,
    approximateArrivalTimestamp: Instant,
    data: T,
    partitionKey: String,
    encryptionType: EncryptionType,
    subSequenceNumber: Long,
    explicitHashKey: String,
    aggregated: Boolean
  )

  /**
   * Staging area for checkpoints
   *
   * Guarantees that the last staged record is checkpointed upon stream shutdown / interruption
   */
  trait Checkpointer {

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
  }

  object Checkpointer {
    private[client] def make(kclCheckpointer: RecordProcessorCheckpointer): UIO[Checkpointer] =
      for {
        latestStaged <- Ref.make[Option[Record[_]]](None)
      } yield new Checkpointer {
        override def stage(r: Record[_]): UIO[Unit] =
          latestStaged.set(Some(r))

        override def checkpoint: ZIO[Blocking, Throwable, Unit] =
          latestStaged.get.flatMap {
            case Some(record) =>
              zio.blocking.blocking {
                Task(kclCheckpointer.checkpoint(record.sequenceNumber, record.subSequenceNumber))
              } *> latestStaged.update {
                case Some(r) if r == record => None
                case r                      => r // A newer record may have been staged by now
              }
            case None         => UIO.unit
          }
      }
  }

}
