package nl.vroste.zio.kinesis.client

import java.time.Instant

import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model._
import zio.clock.Clock
import zio.duration._
import zio.stream.ZStream
import zio.{ Has, Schedule, Task, ZLayer }
import zio.ZIO

object AdminClient {

  val live: ZLayer[Has[KinesisAsyncClient], Throwable, AdminClient] =
    ZLayer.fromService[KinesisAsyncClient, AdminClient.Service](new AdminClientLive(_))

  /**
   * Client for administrative operations
   *
   * The interface is as close as possible to the natural ZIO variant of the KinesisAsyncClient interface,
   * with some noteable differences:
   * - Paginated APIs such as listStreams are modeled as a ZStream
   * - AWS SDK library method responses that only indicate success and do not contain any other
   *   data (besides SDK internals) are mapped to a ZIO of Unit
   * - AWS SDK library method responses that contain a single field are mapped to a ZIO of that field's type
   */
  trait Service {
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

  // Accessors
  def describeStream(
    streamName: String,
    shardLimit: Int = 100,
    exclusiveStartShardId: Option[String] = None
  ): ZIO[AdminClient, Throwable, StreamDescription] =
    ZIO.service[Service].flatMap(_.describeStream(streamName, shardLimit, exclusiveStartShardId))

  final case class DescribeLimitsResponse(shardLimit: Int, openShardCount: Int)

  final case class StreamDescription(
    streamName: String,
    streamARN: String,
    streamStatus: StreamStatus,
    shards: List[Shard],
    hasMoreShards: Boolean,
    retentionPeriodHours: Int,
    streamCreationTimestamp: Instant,
    enhancedMonitoring: List[EnhancedMetrics],
    encryptionType: EncryptionType,
    keyId: String
  )

  final case class StreamDescriptionSummary(
    streamName: String,
    streamARN: String,
    streamStatus: StreamStatus,
    retentionPeriodHours: Int,
    streamCreationTimestamp: Instant,
    enhancedMonitoring: List[EnhancedMetrics],
    encryptionType: EncryptionType,
    keyId: String,
    openShardCount: Int,
    consumerCount: Int
  )

  final case class ConsumerDescription(
    consumerName: String,
    consumerARN: String,
    consumerStatus: ConsumerStatus,
    consumerCreationTimestamp: Instant,
    streamARN: String
  )

  final case class EnhancedMonitoringStatus(
    streamName: String,
    currentShardLevelMetrics: List[MetricsName],
    desiredShardLevelMetrics: List[MetricsName]
  )

  final case class UpdateShardCountResponse(streamName: String, currentShardCount: Int, targetShardCount: Int)

  private[client] val retryOnLimitExceeded = Schedule.recurWhile[Throwable] {
    case _: LimitExceededException => true; case _ => false
  }

  private[client] val defaultBackoffSchedule: Schedule[Clock, Any, Any] = Schedule.exponential(200.millis) && Schedule
    .recurs(5)
}
