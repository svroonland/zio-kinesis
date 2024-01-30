package nl.vroste.zio.kinesis.client.dynamicconsumer

import nl.vroste.zio.kinesis.client.StreamIdentifier
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.kinesis.checkpoint.CheckpointConfig
import software.amazon.kinesis.common.{ ConfigsBuilder, InitialPositionInStream, InitialPositionInStreamExtended }
import software.amazon.kinesis.coordinator.CoordinatorConfig
import software.amazon.kinesis.leases.LeaseManagementConfig
import software.amazon.kinesis.lifecycle.LifecycleConfig
import software.amazon.kinesis.metrics.MetricsConfig
import software.amazon.kinesis.processor.ProcessorConfig
import software.amazon.kinesis.retrieval.RetrievalConfig
import software.amazon.kinesis.retrieval.fanout.FanOutConfig
import software.amazon.kinesis.retrieval.polling.PollingConfig

/**
 * Configuration for the KCL Scheduler
 *
 * Note that its fields are mutable objects
 */
case class SchedulerConfig(
  checkpoint: CheckpointConfig,
  coordinator: CoordinatorConfig,
  leaseManagement: LeaseManagementConfig,
  lifecycle: LifecycleConfig,
  metrics: MetricsConfig,
  processor: ProcessorConfig,
  retrieval: RetrievalConfig,
  initialPositionInStreamExtended: InitialPositionInStreamExtended =
    InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST),
  private val kinesisClient: KinesisAsyncClient,
  private val streamIdentifier: StreamIdentifier
) {
  def withInitialPosition(pos: InitialPositionInStreamExtended): SchedulerConfig =
    copy(
      leaseManagement = leaseManagement.initialPositionInStream(pos),
      initialPositionInStreamExtended = pos
    )

  def withEnhancedFanOut: SchedulerConfig = {
    val config           = new FanOutConfig(kinesisClient)
      .applicationName(retrieval.applicationName())
    val configWithStream = streamIdentifier match {
      case StreamIdentifier.StreamIdentifierByName(streamName) => config.streamName(streamName)
      case StreamIdentifier.StreamIdentifierByArn(_)           => ??? // TODO
    }
    copy(
      retrieval = retrieval.retrievalSpecificConfig(configWithStream)
    )
  }

  def withPolling: SchedulerConfig = {
    val config = streamIdentifier match {
      case StreamIdentifier.StreamIdentifierByName(streamName) => new PollingConfig(streamName, kinesisClient)
      case StreamIdentifier.StreamIdentifierByArn(_)           => ??? // TODO
    }

    copy(
      retrieval = retrieval.retrievalSpecificConfig(
        config
      )
    )
  }

}

object SchedulerConfig {
  def makeDefault(
    builder: ConfigsBuilder,
    kinesisClient: KinesisAsyncClient,
    initialPositionInStreamExtended: InitialPositionInStreamExtended,
    streamIdentifier: StreamIdentifier
  ) =
    SchedulerConfig(
      checkpoint = builder.checkpointConfig(),
      coordinator = builder.coordinatorConfig(),
      leaseManagement = builder.leaseManagementConfig(),
      lifecycle = builder.lifecycleConfig(),
      metrics = builder.metricsConfig(),
      processor = builder.processorConfig(),
      retrieval = builder.retrievalConfig(),
      kinesisClient = kinesisClient,
      streamIdentifier = streamIdentifier
    ).withInitialPosition(initialPositionInStreamExtended)
}
