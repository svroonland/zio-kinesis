package nl.vroste.zio.kinesis.client
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.{ KinesisAsyncClient, KinesisClientBuilder }
import software.amazon.kinesis.common.{ ConfigsBuilder, KinesisClientUtil }
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.lifecycle.events._
import software.amazon.kinesis.processor.ShardRecordProcessor
import software.amazon.kinesis.retrieval.KinesisClientRecord
import software.amazon.kinesis.retrieval.polling.PollingConfig
import zio.blocking.Blocking
import zio.interop.javaz._
import zio.blocking.effectBlocking
import zio.stream.{ ZStream, ZStreamChunk }
import zio.{ Task, ZIO }

object DynamicConsumer {
  def stream(
    streamName: String,
    clientBuilder: KinesisClientBuilder,
    region: Region
  ): ZStream[Blocking, Throwable, (String, ZStreamChunk[Any, Throwable, KinesisClientRecord])] = {

    import java.util.UUID

    val dynamoClient     = DynamoDbAsyncClient.builder.region(region).build
    val cloudWatchClient = CloudWatchAsyncClient.builder.region(region).build
    val kinesisClient    = KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder().region(region));

    def createShardProcessor = new ShardRecordProcessor {
      override def initialize(
        initializationInput: InitializationInput
      ): Unit = ???
      override def processRecords(processRecordsInput: ProcessRecordsInput): Unit =
        processRecordsInput.records()
      override def leaseLost(leaseLostInput: LeaseLostInput): Unit                         = ???
      override def shardEnded(shardEndedInput: ShardEndedInput): Unit                      = ???
      override def shutdownRequested(shutdownRequestedInput: ShutdownRequestedInput): Unit = ???
    }

    val configsBuilder = new ConfigsBuilder(
      streamName,
      streamName,
      kinesisClient,
      dynamoClient,
      cloudWatchClient,
      UUID.randomUUID.toString,
      () => createShardProcessor
    )

    // Run the scheduler
    val schedulerM = for {
      scheduler <- Task(
                    new Scheduler(
                      configsBuilder.checkpointConfig(),
                      configsBuilder.coordinatorConfig(),
                      configsBuilder.leaseManagementConfig(),
                      configsBuilder.lifecycleConfig(),
                      configsBuilder.metricsConfig(),
                      configsBuilder.processorConfig(),
                      configsBuilder
                        .retrievalConfig()
                        .retrievalSpecificConfig(new PollingConfig(streamName, kinesisClient))
                    )
                  ).toManaged_
      stop = effectBlocking(ZIO.fromFutureJava(ZIO(scheduler.startGracefulShutdown()).orDie)).unit.orDie
      _    <- effectBlocking(scheduler.run()).toManaged(_ => stop)
    } yield ()

    ZStream.empty
  }
}
