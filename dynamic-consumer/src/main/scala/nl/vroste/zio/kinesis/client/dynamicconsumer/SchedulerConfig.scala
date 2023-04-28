package nl.vroste.zio.kinesis.client.dynamicconsumer

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
  private val streamName: String
) {
  def withInitialPosition(pos: InitialPositionInStreamExtended): SchedulerConfig =
    copy(
      leaseManagement = leaseManagement.initialPositionInStream(pos),
      initialPositionInStreamExtended = initialPositionInStreamExtended
    )

  def withEnhancedFanOut: SchedulerConfig =
    copy(
      retrieval = retrieval.retrievalSpecificConfig(
        new FanOutConfig(kinesisClient)
          .streamName(streamName)
          .applicationName(retrieval.applicationName())
      )
    )

  def withPolling: SchedulerConfig =
    copy(
      retrieval = retrieval.retrievalSpecificConfig(
        new PollingConfig(streamName, kinesisClient)
      )
    )

}

object SchedulerConfig {
  def makeDefault(
    builder: ConfigsBuilder,
    kinesisClient: KinesisAsyncClient,
    initialPositionInStreamExtended: InitialPositionInStreamExtended,
    streamName: String
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
      streamName = streamName
    ).withInitialPosition(initialPositionInStreamExtended)
}
