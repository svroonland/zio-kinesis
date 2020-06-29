package nl.vroste.zio.kinesis.client

import java.time.Instant

import nl.vroste.zio.kinesis.client.serde.Serializer
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.core.async.SdkPublisher
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.{ ShardIteratorType => JIteratorType, _ }
import zio._
import zio.clock.Clock
import zio.duration._
import zio.interop.reactivestreams._
import zio.stream.ZStream

import scala.jdk.CollectionConverters._
import java.util.concurrent.CompletionException

private[client] class ClientLive(kinesisClient: KinesisAsyncClient) extends Client.Service {
  import Client._
  import Util._

  def createConsumer(streamARN: String, consumerName: String): ZManaged[Any, Throwable, Consumer] =
    registerStreamConsumer(streamARN, consumerName).catchSome {
      case e: ResourceInUseException =>
        // Consumer already exists, retrieve it
        describeStreamConsumer(streamARN, consumerName).map { description =>
          Consumer
            .builder()
            .consumerARN(description.consumerARN())
            .consumerCreationTimestamp(description.consumerCreationTimestamp())
            .consumerName(description.consumerName())
            .consumerStatus(description.consumerStatus())
            .build()
        }.filterOrElse(_.consumerStatus() != ConsumerStatus.DELETING)(_ => ZIO.fail(e))
    }.toManaged(consumer => deregisterStreamConsumer(consumer.consumerARN()).ignore)

  def describeStreamConsumer(
    streamARN: String,
    consumerName: String
  ): ZIO[Any, Throwable, ConsumerDescription] = {
    val request = DescribeStreamConsumerRequest.builder().streamARN(streamARN).consumerName(consumerName).build()
    asZIO(kinesisClient.describeStreamConsumer(request)).map(_.consumerDescription())
  }

  def listShards(
    streamName: String,
    streamCreationTimestamp: Option[Instant] = None,
    chunkSize: Int = 10000
  ): ZStream[Clock, Throwable, Shard] =
    paginatedRequest { (token: Option[String]) =>
      val request = ListShardsRequest
        .builder()
        .maxResults(chunkSize)
        .streamName(streamName)
        .streamCreationTimestamp(streamCreationTimestamp.orNull)
        .nextToken(token.orNull)

      asZIO(kinesisClient.listShards(request.build()))
        .map(response => (response.shards().asScala, Option(response.nextToken())))
    }(Schedule.fixed(10.millis)).mapConcatChunk(Chunk.fromIterable)

  def getShardIterator(
    streamName: String,
    shardId: String,
    iteratorType: ShardIteratorType
  ): Task[String] = {
    val b = GetShardIteratorRequest
      .builder()
      .streamName(streamName)
      .shardId(shardId)

    val request = iteratorType match {
      case ShardIteratorType.Latest                              => b.shardIteratorType(JIteratorType.LATEST)
      case ShardIteratorType.TrimHorizon                         => b.shardIteratorType(JIteratorType.TRIM_HORIZON)
      case ShardIteratorType.AtSequenceNumber(sequenceNumber)    =>
        b.shardIteratorType(JIteratorType.AT_SEQUENCE_NUMBER).startingSequenceNumber(sequenceNumber)
      case ShardIteratorType.AfterSequenceNumber(sequenceNumber) =>
        b.shardIteratorType(JIteratorType.AFTER_SEQUENCE_NUMBER).startingSequenceNumber(sequenceNumber)
      case ShardIteratorType.AtTimestamp(timestamp)              =>
        b.shardIteratorType(JIteratorType.AT_TIMESTAMP).timestamp(timestamp)
    }

    asZIO(kinesisClient.getShardIterator(request.build()))
      .map(_.shardIterator())
  }

  def subscribeToShard(
    consumerARN: String,
    shardID: String,
    startingPosition: ShardIteratorType
  ): ZStream[Any, Throwable, SubscribeToShardEvent] = {

    val b = StartingPosition.builder()

    val jStartingPosition = startingPosition match {
      case ShardIteratorType.Latest                              => b.`type`(JIteratorType.LATEST)
      case ShardIteratorType.TrimHorizon                         => b.`type`(JIteratorType.TRIM_HORIZON)
      case ShardIteratorType.AtSequenceNumber(sequenceNumber)    =>
        b.`type`(JIteratorType.AT_SEQUENCE_NUMBER).sequenceNumber(sequenceNumber)
      case ShardIteratorType.AfterSequenceNumber(sequenceNumber) =>
        b.`type`(JIteratorType.AFTER_SEQUENCE_NUMBER).sequenceNumber(sequenceNumber)
      case ShardIteratorType.AtTimestamp(timestamp)              =>
        b.`type`(JIteratorType.AT_TIMESTAMP).timestamp(timestamp)
    }

    ZStream.unwrap {
      for {
        streamP                 <- Promise.make[Throwable, ZStream[Any, Throwable, SubscribeToShardEvent]]
        streamExceptionOccurred <- Promise.make[Nothing, Throwable]
        runtime                 <- ZIO.runtime[Any]

        subscribeResponse = asZIO {
                              kinesisClient.subscribeToShard(
                                SubscribeToShardRequest
                                  .builder()
                                  .consumerARN(consumerARN)
                                  .shardId(shardID)
                                  .startingPosition(jStartingPosition.build())
                                  .build(),
                                subscribeToShardResponseHandler(runtime, streamP, streamExceptionOccurred)
                              )
                            }
        // subscribeResponse only completes with failure during stream initialization, not with success.
        // It does not contain information of value when succeeding
        _                <- subscribeResponse.unit raceFirst streamP.await raceFirst streamExceptionOccurred.await
        stream           <- (streamExceptionOccurred.await.map(Left(_)) raceFirst streamP.await.either).absolve
      } yield (stream merge ZStream.unwrap(streamExceptionOccurred.await.map(ZStream.fail(_)))).catchSome {
        case e: CompletionException =>
          ZStream.fail(Option(e.getCause).getOrElse(e))
      }
    }
  }

  private def subscribeToShardResponseHandler(
    runtime: zio.Runtime[Any],
    streamP: Promise[Throwable, ZStream[Any, Throwable, SubscribeToShardEvent]],
    streamExceptionOccurred: Promise[Nothing, Throwable]
  ) =
    new SubscribeToShardResponseHandler {
      override def responseReceived(response: SubscribeToShardResponse): Unit =
        ()

      override def onEventStream(publisher: SdkPublisher[SubscribeToShardEventStream]): Unit = {
        val streamOfRecords = publisher.filter(classOf[SubscribeToShardEvent]).toStream()
        runtime.unsafeRun(streamP.succeed(streamOfRecords).unit)
      }

      // For some reason these exceptions are not published by the publisher on onEventStream
      // so we have to merge them into our ZStream ourselves
      override def exceptionOccurred(throwable: Throwable): Unit =
        runtime.unsafeRun(streamExceptionOccurred.succeed(throwable).unit)

      override def complete(): Unit =
        () // We only observe the subscriber's onComplete
    }

  /**
   * @see [[createConsumer]] for automatic deregistration of the consumer
   */
  def registerStreamConsumer(
    streamARN: String,
    consumerName: String
  ): ZIO[Any, Throwable, Consumer] = {
    val request = RegisterStreamConsumerRequest.builder().streamARN(streamARN).consumerName(consumerName).build()
    asZIO(kinesisClient.registerStreamConsumer(request)).map(_.consumer())
  }

  /**
   * @see [[createConsumer]] for automatic deregistration of the consumer
   */
  def deregisterStreamConsumer(consumerARN: String): Task[Unit] = {
    val request = DeregisterStreamConsumerRequest.builder().consumerARN(consumerARN).build()
    asZIO(kinesisClient.deregisterStreamConsumer(request)).unit
  }

  def getRecords(shardIterator: String, limit: Int): Task[GetRecordsResponse] = {
    val request = GetRecordsRequest
      .builder()
      .shardIterator(shardIterator)
      .limit(limit)
      .build()

    asZIO(kinesisClient.getRecords(request))
  }

  private def putRecord(request: PutRecordRequest): Task[PutRecordResponse] =
    asZIO(kinesisClient.putRecord(request))

  private def putRecords(request: PutRecordsRequest): Task[PutRecordsResponse] =
    asZIO(kinesisClient.putRecords(request))

  def putRecord[R, T](
    streamName: String,
    serializer: Serializer[R, T],
    r: ProducerRecord[T]
  ): ZIO[R, Throwable, PutRecordResponse] =
    for {
      dataBytes <- serializer.serialize(r.data)
      request    = PutRecordRequest
                  .builder()
                  .streamName(streamName)
                  .partitionKey(r.partitionKey)
                  .data(SdkBytes.fromByteBuffer(dataBytes))
                  .build()
      response  <- putRecord(request)
    } yield response

  def putRecords[R, T](
    streamName: String,
    serializer: Serializer[R, T],
    records: Iterable[ProducerRecord[T]]
  ): ZIO[R, Throwable, PutRecordsResponse] =
    for {
      recordsAndBytes <- ZIO.foreach(records)(r => serializer.serialize(r.data).map((_, r.partitionKey)))
      entries          = recordsAndBytes.map {
                  case (data, partitionKey) =>
                    PutRecordsRequestEntry
                      .builder()
                      .data(SdkBytes.fromByteBuffer(data))
                      .partitionKey(partitionKey)
                      .build()
                }
      response        <- putRecords(streamName, entries)
    } yield response

  def putRecords(streamName: String, entries: List[PutRecordsRequestEntry]): Task[PutRecordsResponse] =
    putRecords(PutRecordsRequest.builder().streamName(streamName).records(entries: _*).build())

}
