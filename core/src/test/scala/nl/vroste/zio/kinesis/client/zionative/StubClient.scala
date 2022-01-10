package nl.vroste.zio.kinesis.client.zionative
import zio.aws.core.AwsError
import zio.aws.core.aspects.AwsCallAspect
import zio.aws.kinesis.model._
import zio.aws.kinesis.{ model, Kinesis }
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import zio.{ IO, ZEnvironment }
import zio.stream.ZStream

class StubClient extends Kinesis { self =>
  override def withAspect[R](newAspect: AwsCallAspect[R], r: ZEnvironment[R]): Kinesis                                = self
  override val api: KinesisAsyncClient                                                                                = null
  override def splitShard(request: model.SplitShardRequest): IO[AwsError, Unit]                                       = ???
  override def disableEnhancedMonitoring(
    request: model.DisableEnhancedMonitoringRequest
  ): IO[AwsError, DisableEnhancedMonitoringResponse.ReadOnly]                                                         = ???
  override def removeTagsFromStream(request: model.RemoveTagsFromStreamRequest): IO[AwsError, Unit]                   = ???
  override def deleteStream(request: model.DeleteStreamRequest): IO[AwsError, Unit]                                   = ???
  override def describeStream(request: model.DescribeStreamRequest): IO[AwsError, DescribeStreamResponse.ReadOnly]    = ???
  override def decreaseStreamRetentionPeriod(request: model.DecreaseStreamRetentionPeriodRequest): IO[AwsError, Unit] =
    ???

  override def listStreamConsumers(
    request: ListStreamConsumersRequest
  ): ZStream[Any, AwsError, zio.aws.kinesis.model.Consumer.ReadOnly]                                                  = ???
  override def addTagsToStream(request: model.AddTagsToStreamRequest): IO[AwsError, Unit]                             = ???
  override def mergeShards(request: model.MergeShardsRequest): IO[AwsError, Unit]                                     = ???
  override def describeStreamSummary(
    request: model.DescribeStreamSummaryRequest
  ): IO[AwsError, DescribeStreamSummaryResponse.ReadOnly]                                                             = ???
  override def increaseStreamRetentionPeriod(request: model.IncreaseStreamRetentionPeriodRequest): IO[AwsError, Unit] =
    ???
  override def listTagsForStream(
    request: model.ListTagsForStreamRequest
  ): IO[AwsError, ListTagsForStreamResponse.ReadOnly]                                                                 = ???
  override def listShards(request: ListShardsRequest): ZStream[Any, AwsError, Shard.ReadOnly]                         = ???
  override def describeStreamConsumer(
    request: model.DescribeStreamConsumerRequest
  ): IO[AwsError, DescribeStreamConsumerResponse.ReadOnly]                                                            = ???
  override def listStreams(request: model.ListStreamsRequest): IO[AwsError, ListStreamsResponse.ReadOnly]             = ???
  override def putRecord(request: model.PutRecordRequest): IO[AwsError, PutRecordResponse.ReadOnly]                   = ???
  override def updateShardCount(
    request: model.UpdateShardCountRequest
  ): IO[AwsError, UpdateShardCountResponse.ReadOnly]                                                                  = ???
  override def startStreamEncryption(request: model.StartStreamEncryptionRequest): IO[AwsError, Unit]                 = ???
  override def deregisterStreamConsumer(request: model.DeregisterStreamConsumerRequest): IO[AwsError, Unit]           = ???
  override def stopStreamEncryption(request: model.StopStreamEncryptionRequest): IO[AwsError, Unit]                   = ???
  override def putRecords(request: model.PutRecordsRequest): IO[AwsError, PutRecordsResponse.ReadOnly]                = ???
  override def getShardIterator(
    request: model.GetShardIteratorRequest
  ): IO[AwsError, GetShardIteratorResponse.ReadOnly]                                                                  = ???
  override def enableEnhancedMonitoring(
    request: model.EnableEnhancedMonitoringRequest
  ): IO[AwsError, EnableEnhancedMonitoringResponse.ReadOnly]                                                          = ???
  override def createStream(request: model.CreateStreamRequest): IO[AwsError, Unit]                                   = ???
  override def registerStreamConsumer(
    request: model.RegisterStreamConsumerRequest
  ): IO[AwsError, RegisterStreamConsumerResponse.ReadOnly]                                                            = ???
  override def describeLimits(request: model.DescribeLimitsRequest): IO[AwsError, DescribeLimitsResponse.ReadOnly]    = ???
  override def subscribeToShard(
    request: SubscribeToShardRequest
  ): ZStream[Any, AwsError, SubscribeToShardEvent.ReadOnly]                                                           = ???
  override def getRecords(request: model.GetRecordsRequest): IO[AwsError, GetRecordsResponse.ReadOnly]                = ???
  override def listShardsPaginated(request: ListShardsRequest): IO[AwsError, ListShardsResponse.ReadOnly]             = ???
  override def updateStreamMode(request: UpdateStreamModeRequest): IO[AwsError, Unit]                                 = ???
  override def listStreamConsumersPaginated(
    request: ListStreamConsumersRequest
  ): IO[AwsError, ListStreamConsumersResponse.ReadOnly]                                                               = ???
}
