package nl.vroste.zio.kinesis.client

import java.time.Instant
import java.util.UUID

import nl.vroste.zio.kinesis.client.DynamicConsumer.Checkpointer
import nl.vroste.zio.kinesis.client.serde.Deserializer
import software.amazon.awssdk.services.kinesis.model.EncryptionType
import software.amazon.kinesis.common.{ InitialPositionInStream, InitialPositionInStreamExtended }
import zio._
import zio.blocking.Blocking
import zio.stream.ZStream

/**
 * Offers a ZStream based interface to the Amazon Kinesis Client Library (KCL)
 *
 * Ensures proper resource shutdown and failure handling
 */
object DynamicConsumer2 {
  type DynamicConsumer2 = Has[Service]

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
    ]                                   // TODO - what about Blocking?
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

}
