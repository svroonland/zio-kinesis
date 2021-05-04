package nl.vroste.zio.kinesis.client.producer

import io.github.vigoo.zioaws.kinesis.model
import nl.vroste.zio.kinesis.client.ProtobufAggregation
import nl.vroste.zio.kinesis.client.producer.ProducerLive.{
  maxPayloadSizePerRecord,
  payloadSizeForEntryAggregated,
  ProduceRequest
}
import nl.vroste.zio.kinesis.client.zionative.protobuf.Messages
import nl.vroste.zio.kinesis.client.zionative.protobuf.Messages.AggregatedRecord
import zio.{ Chunk, UIO, ZIO }

import scala.jdk.CollectionConverters._

final case class PutRecordsAggregatedBatchForShard(
  entries: Chunk[ProduceRequest],
  payloadSize: Int
) {
  private def builtAggregate: AggregatedRecord = {
    val builder = Messages.AggregatedRecord.newBuilder()

    val records = entries.zipWithIndex.map {
      case (e, index) => ProtobufAggregation.putRecordsRequestEntryToRecord(e.r, index)
    }
    builder
      .addAllRecords(records.asJava)
      .addAllExplicitHashKeyTable(
        entries.map(e => e.r.explicitHashKey.getOrElse("0")).asJava
      ) // TODO optimize: only filled ones
      .addAllPartitionKeyTable(entries.map(e => e.r.partitionKey).asJava)
      .build()
  }

  def add(entry: ProduceRequest): PutRecordsAggregatedBatchForShard =
    copy(entries = entries :+ entry, payloadSize = payloadSize + payloadSizeForEntryAggregated(entry.r))

  def isWithinLimits: Boolean =
    payloadSize <= maxPayloadSizePerRecord

  def toProduceRequest: UIO[ProduceRequest] =
    UIO {
      val r = model.PutRecordsRequestEntry(
        ProtobufAggregation.encodeAggregatedRecord(builtAggregate),
        partitionKey = entries.head.r.partitionKey // First one?
      )

      ProduceRequest(
        r,
        result => ZIO.foreach_(entries)(e => e.complete(result)),
        entries.head.timestamp,
        isAggregated = true,
        aggregateCount = entries.size,
        predictedShard = entries.head.predictedShard
      )
    }
}

object PutRecordsAggregatedBatchForShard {
  val empty: PutRecordsAggregatedBatchForShard = PutRecordsAggregatedBatchForShard(
    Chunk.empty,
    ProtobufAggregation.magicBytes.length + ProtobufAggregation.checksumSize
  )

  def from(r: ProduceRequest): PutRecordsAggregatedBatchForShard =
    empty.add(r)
}
