package nl.vroste.zio.kinesis.client

import java.time.Instant

import nl.vroste.zio.kinesis.client.Util.{ asZIO, paginatedRequest }
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model._
import zio.clock.Clock
import zio.duration._
import zio.interop.reactivestreams._
import zio.stream.ZStream
import zio.{ Tag => _, _ }

import scala.jdk.CollectionConverters._

object AdminClientLive {

  val layer: ZLayer[Has[KinesisAsyncClient], Throwable, AdminClient] =
    ZLayer.fromService[KinesisAsyncClient, AdminClient.Service] { kinesisClient =>
      new AdminClient.Service {
        import AdminClient._
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
              d.enhancedMonitoring().asScala.toList,
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

        def disableEnhancedMonitoring(
          streamName: String,
          metrics: List[MetricsName]
        ): Task[EnhancedMonitoringStatus] = {
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
          streamARN: String,
          streamCreationTimestamp: Option[Instant] = None,
          chunkSize: Int = 10
        ): ZStream[Any, Throwable, Consumer] = {
          val request = ListStreamConsumersRequest
            .builder()
            .maxResults(chunkSize)
            .streamARN(streamARN)
            .streamCreationTimestamp(streamCreationTimestamp.orNull)
            .build()

          ZStream.fromEffect {
            Task(kinesisClient.listStreamConsumersPaginator(request))
          }.flatMap(_.toStream().flatMap(r => ZStream.fromIterable(r.consumers().asScala)))
        }

        def decreaseStreamRetentionPeriod(streamName: String, retentionPeriodHours: Int): Task[Unit] = {
          val request = DecreaseStreamRetentionPeriodRequest
            .builder()
            .streamName(streamName)
            .retentionPeriodHours(retentionPeriodHours)
            .build()
          asZIO(kinesisClient.decreaseStreamRetentionPeriod(request)).unit
        }

        def increaseStreamRetentionPeriod(streamName: String, retentionPeriodHours: Int): Task[Unit] = {
          val request = IncreaseStreamRetentionPeriodRequest
            .builder()
            .streamName(streamName)
            .retentionPeriodHours(retentionPeriodHours)
            .build()
          asZIO(kinesisClient.increaseStreamRetentionPeriod(request)).unit
        }

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
        ): ZStream[Clock, Throwable, String] =
          paginatedRequest { token =>
            val requestBuilder = ListStreamsRequest.builder().limit(chunkSize)

            val requestWithToken =
              token
                .fold(requestBuilder)(requestBuilder.exclusiveStartStreamName)
                .build()

            asZIO(kinesisClient.listStreams(requestWithToken)).map { response =>
              val streamNames = response.streamNames().asScala
              (streamNames, streamNames.lastOption.filter(_ => response.hasMoreStreams))
            }.retry(retryOnLimitExceeded && backoffSchedule)

          }(throttling = Schedule.fixed(200.millis))
            .mapConcatChunk(Chunk.fromIterable)

        def listTagsForStream(
          streamName: String,
          chunkSize: Int = 50,
          backoffSchedule: Schedule[Clock, Throwable, Any] = defaultBackoffSchedule
        ): ZStream[Clock, Throwable, Tag] =
          paginatedRequest { token =>
            val requestBuilder = ListTagsForStreamRequest.builder().streamName(streamName).limit(chunkSize)

            val requestWithToken =
              token
                .fold(requestBuilder)(requestBuilder.exclusiveStartTagKey)
                .build()

            asZIO(kinesisClient.listTagsForStream(requestWithToken)).map { response =>
              val tags = response.tags().asScala
              (tags, tags.lastOption.map(_.key()).filter(_ => response.hasMoreTags))
            }.retry(retryOnLimitExceeded && backoffSchedule)
          }(throttling = Schedule.fixed(200.millis))
            .mapConcatChunk(Chunk.fromIterable)

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
            StartStreamEncryptionRequest
              .builder()
              .streamName(streamName)
              .encryptionType(encryptionType)
              .keyId(keyId)
              .build()
          asZIO(kinesisClient.startStreamEncryption(request)).unit
        }

        def stopStreamEncryption(streamName: String, encryptionType: EncryptionType, keyId: String): Task[Unit] = {
          val request =
            StopStreamEncryptionRequest
              .builder()
              .streamName(streamName)
              .encryptionType(encryptionType)
              .keyId(keyId)
              .build()
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
            .scalingType(scalingType)
            .build()
          asZIO(kinesisClient.updateShardCount(request)).map { r =>
            UpdateShardCountResponse(r.streamName(), r.currentShardCount(), r.targetShardCount())
          }
        }

      }
    }
}
