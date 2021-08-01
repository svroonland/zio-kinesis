package nl.vroste.zio.kinesis.client.dynamicconsumer

import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.kinesis.checkpoint.CheckpointConfig
import software.amazon.kinesis.common.{ ConfigsBuilder, InitialPositionInStreamExtended }
import software.amazon.kinesis.coordinator.CoordinatorConfig
import software.amazon.kinesis.leases.LeaseManagementConfig
import software.amazon.kinesis.lifecycle.LifecycleConfig
import software.amazon.kinesis.metrics.MetricsConfig
import software.amazon.kinesis.processor.ProcessorConfig
import software.amazon.kinesis.retrieval.RetrievalConfig
import software.amazon.kinesis.retrieval.fanout.FanOutConfig
import software.amazon.kinesis.retrieval.polling.PollingConfig

case class SchedulerConfig(
  checkpoint: CheckpointConfig,
  coordinator: CoordinatorConfig,
  leaseManagement: LeaseManagementConfig,
  lifecycle: LifecycleConfig,
  metrics: MetricsConfig,
  processor: ProcessorConfig,
  retrieval: RetrievalConfig,
  private val kinesisClient: KinesisAsyncClient
) {
  def withInitialPosition(pos: InitialPositionInStreamExtended): SchedulerConfig =
    copy(
      leaseManagement = leaseManagement.initialPositionInStream(pos),
      retrieval = retrieval.initialPositionInStreamExtended(pos)
    )

  def withEnhancedFanOut: SchedulerConfig = {
    val streamName = leaseManagement.streamName()
    copy(
      retrieval = retrieval.retrievalSpecificConfig(
        new FanOutConfig(kinesisClient)
          .streamName(streamName)
          .applicationName(retrieval.applicationName())
      )
    )
  }

  def withPolling: SchedulerConfig = {
    val streamName = leaseManagement.streamName()
    copy(
      retrieval = retrieval.retrievalSpecificConfig(
        new PollingConfig(streamName, kinesisClient)
      )
    )
  }

}

object SchedulerConfig {
  def makeDefault(
    builder: ConfigsBuilder,
    kinesisClient: KinesisAsyncClient,
    initialPositionInStreamExtended: InitialPositionInStreamExtended
  ) =
    SchedulerConfig(
      checkpoint = builder.checkpointConfig(),
      coordinator = builder.coordinatorConfig(),
      leaseManagement = builder.leaseManagementConfig(),
      lifecycle = builder.lifecycleConfig(),
      metrics = builder.metricsConfig(),
      processor = builder.processorConfig(),
      retrieval = builder.retrievalConfig(),
      kinesisClient = kinesisClient
    ).withInitialPosition(initialPositionInStreamExtended)
}
