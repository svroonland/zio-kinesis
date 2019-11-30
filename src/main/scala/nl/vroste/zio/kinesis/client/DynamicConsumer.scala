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
import software.amazon.kinesis.retrieval.polling.PollingConfig
import zio._
import zio.blocking.Blocking
import zio.interop.javaz._
import zio.stream.{ StreamChunk, Take, ZStream, ZStreamChunk }

import scala.collection.JavaConverters._

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
   * @tparam R
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
      InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON)
  ): ZStream[Blocking with R, Throwable, (String, ZStreamChunk[Any, Throwable, Record[T]])] = {

    case class ShardQueue(runtime: zio.Runtime[R], q: Queue[Take[Throwable, Chunk[Record[T]]]]) {
      def offerRecords(r: java.util.List[KinesisClientRecord], checkpointer: RecordProcessorCheckpointer): Unit = {
        runtime.unsafeRun {
          ZIO
            .traverse(r.asScala)(r => deserializer.deserialize(r.data()).map((r, _)))
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
            .foldCause(Take.Fail(_), Take.Value(_))
            .flatMap(q.offer)
        }
        ()
      }

      def stop(): Unit = {
        runtime.unsafeRun {
          q.offer(Take.End)
        }
        ()
      }
    }

    class ZioShardProcessor(queues: Queues) extends ShardRecordProcessor {
      var shardQueue: ShardQueue = null

      override def initialize(input: InitializationInput): Unit =
        shardQueue = queues.newShard(input.shardId())

      override def processRecords(processRecordsInput: ProcessRecordsInput): Unit =
        shardQueue.offerRecords(processRecordsInput.records(), processRecordsInput.checkpointer())

      override def leaseLost(leaseLostInput: LeaseLostInput): Unit =
        shardQueue.stop()

      override def shardEnded(shardEndedInput: ShardEndedInput): Unit =
        shardQueue.stop()

      override def shutdownRequested(shutdownRequestedInput: ShutdownRequestedInput): Unit =
        shardQueue.stop()
    }

    class Queues(private val runtime: zio.Runtime[R], val shards: Queue[Take[Throwable, (String, ShardStream[T])]]) {
      def newShard(shard: String): ShardQueue =
        runtime.unsafeRun {
          for {
            queue  <- Queue.unbounded[Take[Throwable, Chunk[Record[T]]]].map(ShardQueue(runtime, _))
            stream = ZStreamChunk(ZStream.fromQueue(queue.q).unTake)
            _      <- shards.offer(Take.Value(shard -> stream)).unit
          } yield queue
        }
    }

    object Queues {
      def make: ZManaged[R, Nothing, Queues] =
        for {
          runtime <- ZIO.runtime[R].toManaged_
          q       <- Queue.unbounded[Take[Throwable, (String, ShardStream[T])]].toManaged(_.shutdown)
        } yield new Queues(runtime, q)
    }

// Run the scheduler
    val schedulerM =
      for {
        queues           <- Queues.make
        kinesisClient    <- ZManaged.fromAutoCloseable(ZIO(kinesisClientBuilder.build()))
        dynamoDbClient   <- ZManaged.fromAutoCloseable(ZIO(dynamoDbClientBuilder.build()))
        cloudWatchClient <- ZManaged.fromAutoCloseable(ZIO(cloudWatchClientBuilder.build()))

        configsBuilder = new ConfigsBuilder(
          streamName,
          applicationName,
          kinesisClient,
          dynamoDbClient,
          cloudWatchClient,
          UUID.randomUUID.toString,
          () => new ZioShardProcessor(queues)
        )
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
                          .retrievalSpecificConfig(new PollingConfig(streamName, kinesisClient))
                      )
                    ).toManaged_
        _ <- ZManaged.fromEffect {
              zio.blocking
                .blocking(ZIO(scheduler.run()))
                .fork
                .flatMap(_.join)
                .onInterrupt(ZIO.fromFutureJava(UIO(scheduler.startGracefulShutdown())).unit.orDie)
            }.fork
      } yield ZStream.fromQueue(queues.shards).unTake

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
  ) = ZStreamChunk {
    shardedStream(
      streamName,
      applicationName,
      deserializer,
      kinesisClientBuilder,
      cloudWatchClientBuilder,
      dynamoDbClientBuilder
    ).flatMapPar(Int.MaxValue)(_._2.chunks)
  }

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

  type ShardStream[T] = StreamChunk[Throwable, Record[T]]
}
