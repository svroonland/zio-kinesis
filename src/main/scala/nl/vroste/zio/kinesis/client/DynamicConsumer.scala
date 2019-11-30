package nl.vroste.zio.kinesis.client
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder
import software.amazon.kinesis.common.{ ConfigsBuilder, KinesisClientUtil }
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.lifecycle.events._
import software.amazon.kinesis.processor.ShardRecordProcessor
import software.amazon.kinesis.retrieval.KinesisClientRecord
import java.util.UUID
import software.amazon.kinesis.retrieval.polling.PollingConfig
import zio._
import zio.blocking.Blocking
import zio.interop.javaz._
import zio.stream.{ StreamChunk, Take, ZStream, ZStreamChunk }

import scala.collection.JavaConverters._

/**
 * ZStream based interface to the Amazon Kinesis Client Library (KCL)
 */
object DynamicConsumer {
  def stream(
    streamName: String,
    applicationName: String,
    clientBuilder: KinesisAsyncClientBuilder,
    region: Region
  ): ZStream[Blocking, Throwable, (String, ZStreamChunk[Any, Throwable, KinesisClientRecord])] = {

    val dynamoClient     = DynamoDbAsyncClient.builder.region(region).build
    val cloudWatchClient = CloudWatchAsyncClient.builder.region(region).build
    val kinesisClient    = KinesisClientUtil.createKinesisAsyncClient(clientBuilder)

    // Run the scheduler
    val schedulerM =
      for {
        queues <- Queues.make

        configsBuilder = new ConfigsBuilder(
          streamName,
          applicationName,
          kinesisClient,
          dynamoClient,
          cloudWatchClient,
          UUID.randomUUID.toString,
          () => new ZioShardProcessor(queues)
        )
        scheduler <- Task(
                      new Scheduler(
                        configsBuilder.checkpointConfig(),
                        configsBuilder.coordinatorConfig(),
                        configsBuilder
                          .leaseManagementConfig()
                          .failoverTimeMillis(1000)
                          .maxLeasesToStealAtOneTime(10),
                        configsBuilder.lifecycleConfig(),
                        configsBuilder.metricsConfig(),
                        configsBuilder.processorConfig(),
                        configsBuilder
                          .retrievalConfig()
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

  class ZioShardProcessor(queues: Queues) extends ShardRecordProcessor {
    var shardQueue: ShardQueue = null

    override def initialize(input: InitializationInput): Unit =
      shardQueue = queues.newShard(input.shardId())

    override def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
      println("Getting records!!!!!")
      // TODO do something with checkpointing

      shardQueue.offerRecords(processRecordsInput.records())
    }

    override def leaseLost(leaseLostInput: LeaseLostInput): Unit = {
      println("Lease lost")
      shardQueue.stop()
    }
    override def shardEnded(shardEndedInput: ShardEndedInput): Unit = {
      println("Shard ended")
      shardQueue.stop()
    }
    override def shutdownRequested(shutdownRequestedInput: ShutdownRequestedInput): Unit = {
      println("Shard shutdown requested")
      shardQueue.stop()
    }
  }

  class Queues(private val runtime: zio.Runtime[Any], val shards: Queue[Take[Throwable, (String, ShardStream)]]) {
    def newShard(shard: String): ShardQueue =
      runtime.unsafeRun {
        for {
          queue  <- Queue.unbounded[Take[Throwable, Chunk[KinesisClientRecord]]].map(ShardQueue(runtime, _))
          stream = ZStreamChunk(ZStream.fromQueue(queue.q).unTake)
          _      = println(s"Adding new shard stream: ${shard}")
          _      <- shards.offer(Take.Value(shard -> stream)).unit
        } yield queue
      }
  }

  object Queues {
    def make: ZManaged[Any, Nothing, Queues] =
      for {
        runtime <- ZIO.runtime[Any].toManaged_
        q       <- Queue.unbounded[Take[Throwable, (String, ShardStream)]].toManaged(_.shutdown)
      } yield new Queues(runtime, q)
  }

  case class ShardQueue(runtime: zio.Runtime[Any], q: Queue[Take[Throwable, Chunk[KinesisClientRecord]]]) {
    def offerRecords(r: java.util.List[KinesisClientRecord]): Unit = {
      runtime.unsafeRun {
        q.offer(Take.Value(Chunk.fromIterable(r.asScala)))
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

  type ShardStream = StreamChunk[Throwable, KinesisClientRecord]
}
