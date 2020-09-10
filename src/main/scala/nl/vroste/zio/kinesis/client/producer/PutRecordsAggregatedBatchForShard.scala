package nl.vroste.zio.kinesis.client.producer

import nl.vroste.zio.kinesis.client.Producer.ProduceResponse
import nl.vroste.zio.kinesis.client.ProtobufAggregation
import nl.vroste.zio.kinesis.client.producer.ProducerLive.{
  maxPayloadSizePerRecord,
  payloadSizeForEntryAggregated,
  ProduceRequest
}
import nl.vroste.zio.kinesis.client.zionative.protobuf.Messages
import nl.vroste.zio.kinesis.client.zionative.protobuf.Messages.AggregatedRecord
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry
import zio.{ Promise, UIO, ZIO }

import scala.jdk.CollectionConverters._

final case class PutRecordsAggregatedBatchForShard(
  entries: List[ProduceRequest],
  payloadSize: Int
) {
  private def builtAggregate: AggregatedRecord = {
    val builder = Messages.AggregatedRecord.newBuilder()

    val entriesInOrder = entries.reverse
    val records        = entriesInOrder.zipWithIndex.map {
      case (e, index) => ProtobufAggregation.putRecordsRequestEntryToRecord(e.r, index)
    }
    val aggregate      = builder
      .addAllRecords(records.asJava)
      .addAllExplicitHashKeyTable(
        entriesInOrder.map(e => Option(e.r.explicitHashKey()).getOrElse("0")).asJava
      ) // TODO optimize: only filled ones
      .addAllPartitionKeyTable(entriesInOrder.map(e => e.r.partitionKey()).asJava)
      .build()

    //      println(
    //        s"Aggregate size: ${aggregate.getSerializedSize}, predicted: ${payloadSize} (${aggregate.getSerializedSize * 100.0 / payloadSize} %)"
    //      )
    aggregate
  }

  def add(entry: ProduceRequest): PutRecordsAggregatedBatchForShard =
    copy(entries = entry +: entries, payloadSize = payloadSize + payloadSizeForEntryAggregated(entry.r))

  def isWithinLimits: Boolean =
    payloadSize <= maxPayloadSizePerRecord

  def toProduceRequest: UIO[ProduceRequest] =
    for {
      done <- Promise.make[Throwable, ProduceResponse]

      r  = PutRecordsRequestEntry
            .builder()
            .partitionKey(entries.head.r.partitionKey()) // First one?
            .data(SdkBytes.fromByteArray(ProtobufAggregation.encodeAggregatedRecord(builtAggregate).toArray))
            .build()
      _ <- ZIO.foreach_(entries)(e => e.done.completeWith(done.await))
    } yield ProduceRequest(
      r,
      done,
      entries.head.timestamp,
      isAggregated = true,
      aggregateCount = entries.size,
      predictedShard = entries.head.predictedShard
    )
}

object PutRecordsAggregatedBatchForShard {
  val empty: PutRecordsAggregatedBatchForShard = PutRecordsAggregatedBatchForShard(
    List.empty,
    ProtobufAggregation.magicBytes.length + ProtobufAggregation.checksumSize
  )

  def from(r: ProduceRequest): PutRecordsAggregatedBatchForShard =
    empty.add(r)
}
