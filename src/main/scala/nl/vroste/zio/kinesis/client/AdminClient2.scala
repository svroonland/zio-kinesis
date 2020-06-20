package nl.vroste.zio.kinesis.client

import java.time.Instant

import software.amazon.awssdk.services.kinesis.model._
import zio.clock.Clock
import zio.stream.ZStream
import zio.{ Has, Schedule, Task }

object AdminClient2 {
  type AdminClient2 = Has[Service]

  trait Service {
    import AdminClient._
    def addTagsToStream(streamName: String, tags: Map[String, String]): Task[Unit]

    def removeTagsFromStream(streamName: String, tagKeys: List[String]): Task[Unit]

    def createStream(name: String, shardCount: Int): Task[Unit]

    def deleteStream(name: String, enforceConsumerDeletion: Boolean = false): Task[Unit]

    def describeLimits: Task[DescribeLimitsResponse]

    def describeStream(
      streamName: String,
      shardLimit: Int = 100,
      exclusiveStartShardId: Option[String] = None
    ): Task[StreamDescription]

    def describeStreamConsumer(consumerARN: String): Task[ConsumerDescription]

    def describeStreamConsumer(streamARN: String, consumerName: String): Task[ConsumerDescription]

    def describeStreamSummary(streamName: String): Task[StreamDescriptionSummary]

    def enableEnhancedMonitoring(streamName: String, metrics: List[MetricsName]): Task[EnhancedMonitoringStatus]

    def disableEnhancedMonitoring(streamName: String, metrics: List[MetricsName]): Task[EnhancedMonitoringStatus]

    def listStreamConsumers(
      streamARN: String,
      streamCreationTimestamp: Option[Instant] = None,
      chunkSize: Int = 10
    ): ZStream[Any, Throwable, Consumer]

    def decreaseStreamRetentionPeriod(streamName: String, retentionPeriodHours: Int): Task[Unit]

    def increaseStreamRetentionPeriod(streamName: String, retentionPeriodHours: Int): Task[Unit]

    /**
     * Lists all streams
     *
     * SDK requests are executed per chunk of `chunkSize` streams. When the response
     * indicates more streams are available, a subsequent request is made, and so on.
     *
     * @param chunkSize The maximum number of streams to retrieve in one request.
     * @param backoffSchedule When requests exceed the rate limit,
     */
    def listStreams(
      chunkSize: Int = 10,
      backoffSchedule: Schedule[Clock, Throwable, Any] = defaultBackoffSchedule
    ): ZStream[Clock, Throwable, String]

    def listTagsForStream(
      streamName: String,
      chunkSize: Int = 50,
      backoffSchedule: Schedule[Clock, Throwable, Any] = defaultBackoffSchedule
    ): ZStream[Clock, Throwable, Tag]

    def mergeShards(streamName: String, shardToMerge: String, adjacentShardToMerge: String): Task[Unit]

    def splitShards(streamName: String, shardToSplit: String, newStartingHashKey: String): Task[Unit]

    def startStreamEncryption(streamName: String, encryptionType: EncryptionType, keyId: String): Task[Unit]

    def stopStreamEncryption(streamName: String, encryptionType: EncryptionType, keyId: String): Task[Unit]

    def updateShardCount(
      streamName: String,
      targetShardCount: Int,
      scalingType: ScalingType = ScalingType.UNIFORM_SCALING
    ): Task[UpdateShardCountResponse]

  }
}
