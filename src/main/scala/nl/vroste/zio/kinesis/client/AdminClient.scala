package nl.vroste.zio.kinesis.client
import java.time.Instant

import nl.vroste.zio.kinesis.client.AdminClient.{ DescribeLimitsResponse, StreamDescription }
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

  def describeStreamConsumer(request: DescribeStreamConsumerRequest): Task[DescribeStreamConsumerResponse] =
    asZIO(kinesisClient.describeStreamConsumer(request))

  def describeStreamSummary(request: DescribeStreamSummaryRequest): Task[DescribeStreamSummaryResponse] =
    asZIO(kinesisClient.describeStreamSummary(request))

  def enableEnhancedMonitoring(request: EnableEnhancedMonitoringRequest): Task[EnableEnhancedMonitoringResponse] =
    asZIO(kinesisClient.enableEnhancedMonitoring(request))

  def disableEnhancedMonitoring(request: DisableEnhancedMonitoringRequest): Task[DisableEnhancedMonitoringResponse] =
    asZIO(kinesisClient.disableEnhancedMonitoring(request))

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

  def mergeShards(request: MergeShardsRequest): Task[Unit] =
    asZIO(kinesisClient.mergeShards(request)).unit

  def splitShards(request: SplitShardRequest): Task[Unit] =
    asZIO(kinesisClient.splitShard(request)).unit

  def startStreamEncryption(request: StartStreamEncryptionRequest): Task[Unit] =
    asZIO(kinesisClient.startStreamEncryption(request)).unit

  def stopStreamEncryption(request: StopStreamEncryptionRequest): Task[Unit] =
    asZIO(kinesisClient.stopStreamEncryption(request)).unit

  def updateShardCount(request: UpdateShardCountRequest): Task[UpdateShardCountResponse] =
    asZIO(kinesisClient.updateShardCount(request))
}

object AdminClient {

  /**
   * Create a client with the region and credentials from the default providers
   *
   * @return Managed resource that is closed after use
   */
  def create: ZManaged[Any, Throwable, AdminClient] =
    ZManaged.fromAutoCloseable {
      ZIO.effect(KinesisAsyncClient.create())
    }.map(new AdminClient(_))

  /**
   * Create a custom client
   *
   * @return Managed resource that is closed after use
   */
  def build(builder: KinesisAsyncClientBuilder): ZManaged[Any, Throwable, AdminClient] =
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
}
