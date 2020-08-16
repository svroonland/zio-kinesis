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

  def putRecords(streamName: String, entries: Iterable[PutRecordsRequestEntry]): Task[PutRecordsResponse] =
    putRecords(PutRecordsRequest.builder().streamName(streamName).records(entries.toArray: _*).build())
}
