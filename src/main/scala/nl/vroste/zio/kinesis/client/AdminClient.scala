package nl.vroste.zio.kinesis.client
import nl.vroste.zio.kinesis.client.Util.asZIO
import software.amazon.awssdk.services.kinesis.{ KinesisAsyncClient, KinesisAsyncClientBuilder }
import software.amazon.awssdk.services.kinesis.model._
import zio.stream.ZStream
import zio.{ Task, ZIO, ZManaged }
import zio.interop.reactiveStreams._

class AdminClient(val kinesisClient: KinesisAsyncClient) {
  def addTagsToStream(request: AddTagsToStreamRequest): Task[Unit] =
    asZIO(kinesisClient.addTagsToStream(request)).unit

  def removeTagsFromStream(request: RemoveTagsFromStreamRequest): Task[Unit] =
    asZIO(kinesisClient.removeTagsFromStream(request)).unit

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

  def deleteStream(request: DeleteStreamRequest): Task[Unit] =
    asZIO(kinesisClient.deleteStream(request)).unit

  def describeLimits: Task[DescribeLimitsResponse] =
    asZIO(kinesisClient.describeLimits())

  def describeLimits(request: DescribeLimitsRequest): Task[DescribeLimitsResponse] =
    asZIO(kinesisClient.describeLimits(request))

  def describeStream(request: DescribeStreamRequest): Task[DescribeStreamResponse] =
    asZIO(kinesisClient.describeStream(request))

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
}
