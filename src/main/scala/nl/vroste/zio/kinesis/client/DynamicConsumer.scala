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
   * @param isEnhancedFanOut Flag for setting retrieval config - defaults to `true`. If `false` polling config is set.
   * @param leaseTableName Optionally set the lease table name - defaults to None. If not specified the `applicationName` will be used.
   * @tparam R ZIO environment type required by the `deserializer`
   * @tparam T Type of record values
   * @return
   */
  def shardedStream[R, T](
    streamName: String,
    applicationName: String,
    deserializer: Deserializer[R, T],
    kinesisClientBuilder: KinesisAsyncClientBuilder = KinesisAsyncClient.builder(),
    cloudWatchClientBuilder: CloudWatchAsyncClientBuilder = CloudWatchAsyncClient.builder,
    dynamoDbClientBuilder: DynamoDbAsyncClientBuilder = DynamoDbAsyncClient.builder(),
    initialPosition: InitialPositionInStreamExtended =
      InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON),
    isEnhancedFanOut: Boolean = true,
    leaseTableName: Option[String] = None
  ): ZStream[Blocking with R, Throwable, (String, ZStream[Any, Throwable, Record[T]])] = {

    /**
     * A queue for a single Shard and interface between the KCL threadpool and the ZIO runtime
     *
     * This queue is used by a ZStream for a single Shard
     *
     * The Queue uses the error channel (E type parameter) to signal failure (Some[Throwable])
     * and completion (None)
     *
     * @param shardId Shard identifier
     * @param runtime ZIO runtime
     * @param q Queue for record chunks
     */
    case class ShardQueue(
      shardId: String,
      runtime: zio.Runtime[R],
      q: Queue[Exit[Option[Throwable], Chunk[Record[T]]]]
    ) {
      def offerRecords(r: java.util.List[KinesisClientRecord], checkpointer: RecordProcessorCheckpointer): Unit =
        // TODO only offer to the queue in unsafeRun, run the rest 'within' the regular ZIO runtime
        runtime.unsafeRun {
          ZIO
            .foreach(r.asScala)(r => deserializer.deserialize(r.data()).map((r, _)))
            .map { records =>
              Chunk.fromIterable(records.map {
                case (r, data) =>
                  Record(
                    r.sequenceNumber(),
                    r.approximateArrivalTimestamp(),
                    data,
                    r.partitionKey(),
                    r.encryptionType(),
                    r.subSequenceNumber(),
                    r.explicitHashKey(),
                    r.aggregated(),
                    checkpoint = zio.blocking.blocking {
                      Task(checkpointer.checkpoint(r.sequenceNumber(), r.subSequenceNumber()))
                    }
                  )
              })
            }
            .fold(e => Exit.fail(Some(e)), Exit.succeed)
            .flatMap(q.offer)
            .unit
        }

      def stop(): Unit =
        runtime.unsafeRun {
          q.offer(Exit.fail(None)).unit
        }
    }

    class ZioShardProcessor(queues: Queues) extends ShardRecordProcessor {
      var shardQueue: ShardQueue = _

      override def initialize(input: InitializationInput): Unit =
        shardQueue = queues.newShard(input.shardId())

      override def processRecords(processRecordsInput: ProcessRecordsInput): Unit =
        shardQueue.offerRecords(processRecordsInput.records(), processRecordsInput.checkpointer())

      override def leaseLost(leaseLostInput: LeaseLostInput): Unit =
        shardQueue.stop()

      override def shardEnded(shardEndedInput: ShardEndedInput): Unit =
        shardQueue.stop()

      override def shutdownRequested(shutdownRequestedInput: ShutdownRequestedInput): Unit =
        // TODO should we wait for processing of this shard to complete?
        shardQueue.stop()
    }

    class Queues(
      private val runtime: zio.Runtime[R],
      val shards: Queue[Exit[Option[Throwable], (String, ShardStream[T])]]
    ) {
      def newShard(shard: String): ShardQueue =
        runtime.unsafeRun {
          for {
            queue <- Queue.unbounded[Exit[Option[Throwable], Chunk[Record[T]]]].map(ShardQueue(shard, runtime, _))
            stream = ZStream.fromQueue(queue.q).collectWhileSuccess.flattenChunks
            _     <- shards.offer(Exit.succeed(shard -> stream)).unit
          } yield queue
        }
    }

    object Queues {
      def make: ZManaged[R, Nothing, Queues] =
        for {
          runtime <- ZIO.runtime[R].toManaged_
          q       <- Queue.unbounded[Exit[Option[Throwable], (String, ShardStream[T])]].toManaged(_.shutdown)
        } yield new Queues(runtime, q)
    }

    def retrievalConfig(kinesisClient: KinesisAsyncClient) =
      if (isEnhancedFanOut)
        new FanOutConfig(kinesisClient).streamName(streamName).applicationName(applicationName)
      else
        new PollingConfig(streamName, kinesisClient)

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
            UUID.randomUUID.toString,
            () => new ZioShardProcessor(queues)
          )
          leaseTableName.fold(configsBuilder)(configsBuilder.tableName)
        }

        scheduler     <- Task(
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
        _             <- zio.blocking
               .blocking(ZIO(scheduler.run()))
               .fork
               .flatMap(_.join)
               .onInterrupt(ZIO.fromFutureJava(scheduler.startGracefulShutdown()).unit.orDie)
               .forkManaged
      } yield ZStream.fromQueue(queues.shards).collectWhileSuccess

    ZStream.unwrapManaged(schedulerM)
  }

  /**
   * Like [[shardedStream]], but merges all shards into one ZStream
   */
  def plainStream[R, T](
    streamName: String,
    applicationName: String,
    deserializer: Deserializer[R, T],
    kinesisClientBuilder: KinesisAsyncClientBuilder = KinesisAsyncClient.builder(),
    cloudWatchClientBuilder: CloudWatchAsyncClientBuilder = CloudWatchAsyncClient.builder,
    dynamoDbClientBuilder: DynamoDbAsyncClientBuilder = DynamoDbAsyncClient.builder()
  ): ZStream[Blocking with R, Throwable, Record[T]] =
    shardedStream(
      streamName,
      applicationName,
      deserializer,
      kinesisClientBuilder,
      cloudWatchClientBuilder,
      dynamoDbClientBuilder
    ).flatMapPar(Int.MaxValue)(_._2)

  case class Record[T](
    sequenceNumber: String,
    approximateArrivalTimestamp: Instant,
    data: T,
    partitionKey: String,
    encryptionType: EncryptionType,
    subSequenceNumber: Long,
    explicitHashKey: String,
    aggregated: Boolean,
    checkpoint: ZIO[Blocking, Throwable, Unit]
  )

  type ShardStream[T] = ZStream[Any, Throwable, Record[T]]
}
