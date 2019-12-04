package nl.vroste.zio.kinesis.client
import java.time.Instant

import nl.vroste.zio.kinesis.client.AdminClient.{
  ConsumerDescription,
  DescribeLimitsResponse,
  EnhancedMonitoringStatus,
  StreamDescription,
  StreamDescriptionSummary,
  UpdateShardCountResponse
}
import nl.vroste.zio.kinesis.client.Util.asZIO
import software.amazon.awssdk.services.kinesis.model._
import software.amazon.awssdk.services.kinesis.{ KinesisAsyncClient, KinesisAsyncClientBuilder }
import zio.interop.reactiveStreams._
import zio.stream.ZStream
import zio.{ Task, ZIO, ZManaged }

import scala.collection.JavaConverters._

class AdminClient(val kinesisClient: KinesisAsyncClient) {
  def addTagsToStream(streamName: String, tags: Map[String, String]): Task[Unit] = {
    val request = AddTagsToStreamRequest.builder().streamName(streamName).tags(tags.asJava).build()
    asZIO(kinesisClient.addTagsToStream(request)).unit
  }

  def removeTagsFromStream(streamName: String, tagKeys: List[String]): Task[Unit] = {
    val request = RemoveTagsFromStreamRequest.builder().streamName(streamName).tagKeys(tagKeys.asJava).build()
    asZIO(kinesisClient.removeTagsFromStream(request)).unit
  }

  def createStream(name: String, shardCount: Int): Task[Unit] = {
    val request = CreateStreamRequest
      .builder()
      .streamName(name)
      .shardCount(shardCount)
      .build()

    asZIO(kinesisClient.createStream(request)).unit
  }

  def deleteStream(name: String, enforceConsumerDeletion: Boolean = false): Task[Unit] = {
    val request =
      DeleteStreamRequest
        .builder()
        .streamName(name)
        .enforceConsumerDeletion(enforceConsumerDeletion)
        .build()

    asZIO(kinesisClient.deleteStream(request)).unit
  }

  def describeLimits: Task[DescribeLimitsResponse] =
    asZIO(kinesisClient.describeLimits())
      .map(r => DescribeLimitsResponse(r.shardLimit(), r.openShardCount()))

  def describeStream(
    streamName: String,
    shardLimit: Int = 100,
    exclusiveStartShardId: Option[String] = None
  ): Task[StreamDescription] = {
    val request = DescribeStreamRequest
      .builder()
      .streamName(streamName)
      .limit(shardLimit)
      .exclusiveStartShardId(exclusiveStartShardId.orNull)
      .build()

    asZIO(kinesisClient.describeStream(request)).map { r =>
      val d = r.streamDescription()
      StreamDescription(
        d.streamName(),
        d.streamARN(),
        d.streamStatus(),
        d.shards().asScala.toList,
        d.hasMoreShards,
        d.retentionPeriodHours(),
        d.streamCreationTimestamp(),
        d.enhancedMonitoring().asScala.toList,
        d.encryptionType(),
        d.keyId()
      )
    }
  }

  def describeStreamConsumer(consumerARN: String): Task[ConsumerDescription] =
    describeStreamConsumer(DescribeStreamConsumerRequest.builder().consumerARN(consumerARN).build())

  def describeStreamConsumer(streamARN: String, consumerName: String): Task[ConsumerDescription] =
    describeStreamConsumer(
      DescribeStreamConsumerRequest.builder().streamARN(streamARN).consumerName(consumerName).build()
    )

  private def describeStreamConsumer(request: DescribeStreamConsumerRequest) =
    asZIO(kinesisClient.describeStreamConsumer(request)).map { r =>
      val d = r.consumerDescription()

      ConsumerDescription(
        d.consumerName(),
        d.consumerARN(),
        d.consumerStatus(),
        d.consumerCreationTimestamp(),
        d.streamARN()
      )
    }

  def describeStreamSummary(streamName: String): Task[StreamDescriptionSummary] = {
    val request = DescribeStreamSummaryRequest.builder().streamName(streamName).build()
    asZIO(kinesisClient.describeStreamSummary(request)).map { r =>
      val d = r.streamDescriptionSummary()

      StreamDescriptionSummary(
        d.streamName(),
        d.streamARN(),
        d.streamStatus(),
        d.retentionPeriodHours(),
        d.streamCreationTimestamp(),
        d.enhancedMonitoring(),
        d.encryptionType(),
        d.keyId(),
        d.openShardCount(),
        d.consumerCount()
      )
    }
  }

  def enableEnhancedMonitoring(streamName: String, metrics: List[MetricsName]): Task[EnhancedMonitoringStatus] = {
    val request =
      EnableEnhancedMonitoringRequest.builder().streamName(streamName).shardLevelMetrics(metrics: _*).build()

    asZIO(kinesisClient.enableEnhancedMonitoring(request)).map { r =>
      EnhancedMonitoringStatus(
        r.streamName(),
        r.currentShardLevelMetrics().asScala.toList,
        r.desiredShardLevelMetrics().asScala.toList
      )
    }
  }

  def disableEnhancedMonitoring(streamName: String, metrics: List[MetricsName]): Task[EnhancedMonitoringStatus] = {
    val request =
      DisableEnhancedMonitoringRequest.builder().streamName(streamName).shardLevelMetrics(metrics: _*).build()

    asZIO(kinesisClient.disableEnhancedMonitoring(request)).map { r =>
      EnhancedMonitoringStatus(
        r.streamName(),
        r.currentShardLevelMetrics().asScala.toList,
        r.desiredShardLevelMetrics().asScala.toList
      )
    }
  }

  def listStreamConsumers(
    request: ListStreamConsumersRequest
  ): ZIO[Any, Throwable, ZStream[Any, Throwable, ListStreamConsumersResponse]] =
    Task(kinesisClient.listStreamConsumersPaginator(request)).map(_.toStream())

  def decreaseStreamRetentionPeriod(
    request: DecreaseStreamRetentionPeriodRequest
  ): Task[Unit] =
    asZIO(kinesisClient.decreaseStreamRetentionPeriod(request)).unit

  def increaseStreamRetentionPeriod(
    request: IncreaseStreamRetentionPeriodRequest
  ): Task[Unit] =
    asZIO(kinesisClient.increaseStreamRetentionPeriod(request)).unit

  def listStreams(request: ListStreamsRequest): Task[ListStreamsResponse] =
    asZIO(kinesisClient.listStreams(request))

  def listTagsForStream(request: ListTagsForStreamRequest): Task[ListTagsForStreamResponse] =
    asZIO(kinesisClient.listTagsForStream(request))

  def mergeShards(streamName: String, shardToMerge: String, adjacentShardToMerge: String): Task[Unit] = {
    val request = MergeShardsRequest
      .builder()
      .streamName(streamName)
      .shardToMerge(shardToMerge)
      .adjacentShardToMerge(adjacentShardToMerge)
      .build()
    asZIO(kinesisClient.mergeShards(request)).unit
  }

  def splitShards(streamName: String, shardToSplit: String, newStartingHashKey: String): Task[Unit] = {
    val request = SplitShardRequest
      .builder()
      .streamName(streamName)
      .shardToSplit(shardToSplit)
      .newStartingHashKey(newStartingHashKey)
      .build()
    asZIO(kinesisClient.splitShard(request)).unit
  }

  def startStreamEncryption(streamName: String, encryptionType: EncryptionType, keyId: String): Task[Unit] = {
    val request =
      StartStreamEncryptionRequest.builder().streamName(streamName).encryptionType(encryptionType).keyId(keyId).build()
    asZIO(kinesisClient.startStreamEncryption(request)).unit
  }

  def stopStreamEncryption(streamName: String, encryptionType: EncryptionType, keyId: String): Task[Unit] = {
    val request =
      StopStreamEncryptionRequest.builder().streamName(streamName).encryptionType(encryptionType).keyId(keyId).build()
    asZIO(kinesisClient.stopStreamEncryption(request)).unit
  }

  def updateShardCount(
    streamName: String,
    targetShardCount: Int,
    scalingType: ScalingType = ScalingType.UNIFORM_SCALING
  ): Task[UpdateShardCountResponse] = {
    val request = UpdateShardCountRequest
      .builder()
      .streamName(streamName)
      .targetShardCount(targetShardCount)
      .scalingType(ScalingType)
      .build()
    asZIO(kinesisClient.updateShardCount(request)).map { r =>
      UpdateShardCountResponse(r.streamName(), r.currentShardCount(), r.targetShardCount())
    }
  }
}

object AdminClient {

  /**
   * Create a client with the region and credentials from the default providers
   *
   * @return Managed resource that is closed after use
   */
  def create: ZManaged[Any, Throwable, AdminClient] = build()

  /**
   * Create a custom client
   *
   * @param builder Client builder
   * @return Managed resource that is closed after use
   */
  def build(builder: KinesisAsyncClientBuilder = KinesisAsyncClient.builder()): ZManaged[Any, Throwable, AdminClient] =
    ZManaged.fromAutoCloseable {
      ZIO.effect(builder.build())
    }.map(new AdminClient(_))

  case class DescribeLimitsResponse(shardLimit: Int, openShardCount: Int)

  case class StreamDescription(
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

  case class StreamDescriptionSummary(
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

  case class ConsumerDescription(
    consumerName: String,
    consumerARN: String,
    consumerStatus: ConsumerStatus,
    consumerCreationTimestamp: Instant,
    streamARN: String
  )

  case class EnhancedMonitoringStatus(
    streamName: String,
    currentShardLevelMetrics: List[MetricsName],
    desiredShardLevelMetrics: List[MetricsName]
  )

  case class UpdateShardCountResponse(streamName: String, currentShardCount: Int, targetShardCount: Int)
}
