package nl.vroste.zio.kinesis.client.producer

import nl.vroste.zio.kinesis.client.ProtobufAggregation
import nl.vroste.zio.kinesis.client.producer.ProducerLive.{
  maxPayloadSizePerRecord,
  payloadSizeForEntryAggregated,
  ProduceRequest
}
import nl.vroste.zio.kinesis.client.zionative.protobuf.Messages
import nl.vroste.zio.kinesis.client.zionative.protobuf.Messages.AggregatedRecord
import zio.aws.kinesis.model.primitives.PartitionKey
import zio.{ Chunk, UIO, ZIO }

import java.security.MessageDigest
import scala.jdk.CollectionConverters._

final case class PutRecordsAggregatedBatchForShard(
  entries: Chunk[ProduceRequest],
  payloadSize: Int
) {
  private def builtAggregate: AggregatedRecord = {
    val builder = Messages.AggregatedRecord.newBuilder()

    // Hand-rolled
    var index = 0
    val it    = entries.iterator
    while (it.hasNext) {
      val e = it.next()

      val record = ProtobufAggregation.putRecordsRequestEntryToRecord(e.data, None, index)
      builder.addRecords(record)

      index = index + 1
    }

    builder
      .addAllExplicitHashKeyTable(
        entries.map(_ => "0").asJava
      ) // TODO optimize: only filled ones
      .addAllPartitionKeyTable(entries.map(e => PartitionKey.unwrap(e.partitionKey)).asJava)
      .build()
  }

  def add(entry: ProduceRequest): PutRecordsAggregatedBatchForShard =
    copy(entries = entries :+ entry, payloadSize = payloadSize + payloadSizeForEntryAggregated(entry))

  def isWithinLimits: Boolean =
    payloadSize <= maxPayloadSizePerRecord

  def toProduceRequest(digest: MessageDigest): UIO[ProduceRequest] =
    UIO {
      // Do not inline to avoid capturing the entire chunk in the closure below
      val completes = entries.map(_.complete)

      ProduceRequest(
        data = ProtobufAggregation.encodeAggregatedRecord(digest, builtAggregate),
        partitionKey = entries.head.partitionKey, // First one?
        complete = result => ZIO.foreachDiscard(completes)(_(result)),
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
