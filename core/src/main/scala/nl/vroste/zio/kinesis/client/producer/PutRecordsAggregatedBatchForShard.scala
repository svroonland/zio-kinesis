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
      case (e, index) => ProtobufAggregation.putRecordsRequestEntryToRecord(e.data, e.partitionKey, None, index)
    }
    builder
      .addAllRecords(records.asJava)
      .addAllExplicitHashKeyTable(
        entries.map(_ => "0").asJava
      ) // TODO optimize: only filled ones
      .addAllPartitionKeyTable(entries.map(e => e.partitionKey).asJava)
      .build()
  }

  def add(entry: ProduceRequest): PutRecordsAggregatedBatchForShard =
    copy(entries = entries :+ entry, payloadSize = payloadSize + payloadSizeForEntryAggregated(entry))

  def isWithinLimits: Boolean =
    payloadSize <= maxPayloadSizePerRecord

  def toProduceRequest: UIO[ProduceRequest] =
    UIO {
      // Do not inline to avoid capturing the entire chunk in the closure below
      val completes = entries.map(_.complete)

      ProduceRequest(
        data = ProtobufAggregation.encodeAggregatedRecord(builtAggregate),
        partitionKey = entries.head.partitionKey, // First one?
        complete = result => ZIO.foreach_(completes)(_(result)),
        timestamp = entries.head.timestamp,
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
