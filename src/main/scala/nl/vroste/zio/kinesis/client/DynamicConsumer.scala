package nl.vroste.zio.kinesis.client
import java.time.Instant
import java.util.UUID

import nl.vroste.zio.kinesis.client.serde.Deserializer
import software.amazon.awssdk.services.cloudwatch.{ CloudWatchAsyncClient, CloudWatchAsyncClientBuilder }
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient, DynamoDbAsyncClientBuilder }
import software.amazon.awssdk.services.kinesis.model.EncryptionType
import software.amazon.awssdk.services.kinesis.{ KinesisAsyncClient, KinesisAsyncClientBuilder }
import software.amazon.kinesis.common.{ ConfigsBuilder, InitialPositionInStream, InitialPositionInStreamExtended }
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.lifecycle.events._
import software.amazon.kinesis.processor.{ RecordProcessorCheckpointer, ShardRecordProcessor }
import software.amazon.kinesis.retrieval.KinesisClientRecord
import software.amazon.kinesis.retrieval.fanout.FanOutConfig
import software.amazon.kinesis.retrieval.polling.PollingConfig
import zio._
import zio.blocking.Blocking
import zio.stream.ZStream

import scala.jdk.CollectionConverters._
import software.amazon.kinesis.exceptions.ShutdownException

/**
 * Offers a ZStream based interface to the Amazon Kinesis Client Library (KCL)
 *
 * Ensures proper resource shutdown and failure handling
 */
object DynamicConsumer {

  /**
   * Create a ZStream of records on a Kinesis stream with substreams per shard
   *
   * Uses DynamoDB for lease coordination between different instances of consumers
   * with the same application name and for offset checkpointing.
   *
   * @param streamName Name of the Kinesis stream
   * @param applicationName Application name for coordinating shard leases
   * @param deserializer Deserializer for record values
   * @param kinesisClientBuilder
   * @param cloudWatchClientBuilder
   * @param dynamoDbClientBuilder
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
    deserializer: Deserializer[R, T],
    kinesisClientBuilder: KinesisAsyncClientBuilder = KinesisAsyncClient.builder(),
    cloudWatchClientBuilder: CloudWatchAsyncClientBuilder = CloudWatchAsyncClient.builder,
    dynamoDbClientBuilder: DynamoDbAsyncClientBuilder = DynamoDbAsyncClient.builder(),
    requestShutdown: UIO[Unit] = UIO.never,
    initialPosition: InitialPositionInStreamExtended =
      InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON),
    isEnhancedFanOut: Boolean = true,
    leaseTableName: Option[String] = None,
    workerIdentifier: String = UUID.randomUUID().toString,
    maxShardBufferSize: Int = 1024 // Prefer powers of 2
  ): ZStream[Blocking with R, Throwable, (String, ZStream[Blocking, Throwable, Record[T]], Checkpointer)] = {
    /*
     * A queue for a single Shard and interface between the KCL threadpool and the ZIO runtime
     *
     * This queue is used by a ZStream for a single Shard
     *
     * The Queue uses the error channel (E type parameter) to signal failure (Some[Throwable])
     * and completion (None)
     */
    class ShardQueue(
      val shardId: String,
      runtime: zio.Runtime[Any],
      val q: Queue[Exit[Option[Throwable], KinesisClientRecord]]
    ) {
      def offerRecords(r: java.util.List[KinesisClientRecord]): Unit =
        // Calls to q.offer will fail with an interruption error after the queue has been shutdown
        // TODO we must make sure never to throw an exception here, because KCL will delete the records
        // See https://github.com/awslabs/amazon-kinesis-client/issues/10
        runtime.unsafeRun {
          UIO(println(s"ShardQueue: offerRecords for ${shardId} got ${r.size()} records")) *>
            q.offerAll(r.asScala.map(Exit.succeed(_))).unit.catchSomeCause {
              case c if c.interrupted => ZIO.unit
            } *> // TODO what behavior do we want if the queue + substream are already shutdown for some reason..?
            UIO(println(s"ShardQueue: offerRecords for ${shardId} COMPLETE"))
        }

      def shutdownQueue: UIO[Unit] =
        UIO(println(s"ShardQueue: shutdownQueue for ${shardId}")) *>
          q.shutdown

      /**
       * Shutdown processing for this shard
       *
       * Clear everything that is still in the queue, offer a completion signal for the queue,
       * set an interrupt signal and await stream completion (in-flight messages processed)
       *
       */
      def stop(reason: String): Unit =
        runtime.unsafeRun {
          UIO(println(s"ShardQueue: stop() for ${shardId} because of ${reason}")).when(true) *>
            (
              q.takeAll.unit *>                  // Clear the queue so it doesn't have to be drained fully
                q.offer(Exit.fail(None)).unit <* // Pass an exit signal in the queue to stop the stream
                q.awaitShutdown
            ).race(
              q.awaitShutdown
            ) <* // Wait for the stream's end to 'bubble up', meaning all in-flight elements have been processed
            UIO(println(s"ShardQueue: stop() for ${shardId} because of ${reason} - COMPLETE")).when(true)
          // TODO maybe we want to only do this when the main stream's completion has bubbled up..?
        }
    }

    class ZioShardProcessor(queues: Queues) extends ShardRecordProcessor {
      var shardId: String        = _
      var shardQueue: ShardQueue = _

      override def initialize(input: InitializationInput): Unit =
        shardId = input.shardId()

      override def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
        if (shardQueue == null)
          shardQueue = queues.newShard(shardId, processRecordsInput.checkpointer())
        shardQueue.offerRecords(processRecordsInput.records())
      }

      override def leaseLost(leaseLostInput: LeaseLostInput): Unit =
        shardQueue.stop("lease lost")

      override def shardEnded(shardEndedInput: ShardEndedInput): Unit =
        shardQueue.stop("shard ended")

      override def shutdownRequested(shutdownRequestedInput: ShutdownRequestedInput): Unit =
        shardQueue.stop("shutdown requested")
    }

    class Queues(
      private val runtime: zio.Runtime[Any],
      val shards: Queue[Exit[Option[Throwable], (String, ShardQueue, Checkpointer)]]
    ) {
      def newShard(shard: String, checkpointer: RecordProcessorCheckpointer): ShardQueue =
        runtime.unsafeRun {
          for {
            queue        <- Queue
                       .bounded[Exit[Option[Throwable], KinesisClientRecord]](
                         maxShardBufferSize
                       )
                       .map(new ShardQueue(shard, runtime, _))
            checkpointer <- Checkpointer.make(checkpointer)
            _            <- shards.offer(Exit.succeed((shard, queue, checkpointer))).unit
          } yield queue
        }

      def shutdown: UIO[Unit] =
        shards.offer(Exit.fail(None)).unit
    }

    object Queues {
      def make: ZManaged[Any, Nothing, Queues] =
        for {
          runtime <- ZIO.runtime[Any].toManaged_
          q       <- Queue.unbounded[Exit[Option[Throwable], (String, ShardQueue, Checkpointer)]].toManaged(_.shutdown)
        } yield new Queues(runtime, q)
    }

    def retrievalConfig(kinesisClient: KinesisAsyncClient) =
      if (isEnhancedFanOut)
        new FanOutConfig(kinesisClient).streamName(streamName).applicationName(applicationName)
      else
        new PollingConfig(streamName, kinesisClient)

    def toRecord(
      shardId: String,
      r: KinesisClientRecord
    ): ZIO[R, Throwable, Record[T]] =
      deserializer.deserialize(r.data()).map { data =>
        Record(
          shardId,
          r.sequenceNumber(),
          r.approximateArrivalTimestamp(),
          data,
          r.partitionKey(),
          r.encryptionType(),
          r.subSequenceNumber(),
          r.explicitHashKey(),
          r.aggregated()
        )
      }

    // Run the scheduler
    val schedulerM =
      for {
        queues           <- Queues.make
        kinesisClient    <- ZManaged.fromAutoCloseable(ZIO(kinesisClientBuilder.build()))
        dynamoDbClient   <- ZManaged.fromAutoCloseable(ZIO(dynamoDbClientBuilder.build()))
        cloudWatchClient <- ZManaged.fromAutoCloseable(ZIO(cloudWatchClientBuilder.build()))

        configsBuilder = {
          val configsBuilder = new ConfigsBuilder(
            streamName,
            applicationName,
            kinesisClient,
            dynamoDbClient,
            cloudWatchClient,
            workerIdentifier,
            () => new ZioShardProcessor(queues)
          )
          leaseTableName.fold(configsBuilder)(configsBuilder.tableName)
        }
        env           <- ZIO.environment[R].toManaged_

        scheduler <- Task(
                       new Scheduler(
                         configsBuilder.checkpointConfig(),
                         configsBuilder.coordinatorConfig(),
                         configsBuilder.leaseManagementConfig().initialPositionInStream(initialPosition),
                         configsBuilder.lifecycleConfig(),
                         configsBuilder.metricsConfig(),
                         configsBuilder.processorConfig(),
                         configsBuilder
                           .retrievalConfig()
                           .initialPositionInStreamExtended(initialPosition)
                           .retrievalSpecificConfig(retrievalConfig(kinesisClient))
                       )
                     ).toManaged_
        doShutdown = // UIO(println("Starting graceful shutdown")) *>
                     ZIO.fromFutureJava(scheduler.startGracefulShutdown()).unit.orDie <*
                       queues.shutdown
        _         <- zio.blocking
               .blocking(ZIO(scheduler.run()))
               .fork
               .flatMap(_.join)
               .onInterrupt(doShutdown)
               .forkManaged
        _         <- (requestShutdown *> doShutdown).forkManaged
      } yield ZStream
        .fromQueue(queues.shards)
        .collectWhileSuccess
        .map {
          case (shardId, shardQueue, checkpointer) =>
            val stream = ZStream
              .fromQueue(shardQueue.q)
              .ensuringFirst(shardQueue.shutdownQueue)
              .collectWhileSuccess
              .mapChunksM(_.mapM(toRecord(shardId, _)))
              .provide(env)
              .ensuringFirst(checkpointer.checkpoint.catchSome { case _: ShutdownException => UIO.unit }.orDie)

            (shardId, stream, checkpointer)
        }

    ZStream.unwrapManaged(schedulerM)
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

  type ShardStream[T] = ZStream[Any, Throwable, Record[T]]

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
