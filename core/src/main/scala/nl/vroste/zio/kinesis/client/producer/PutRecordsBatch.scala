package nl.vroste.zio.kinesis.client.producer
import nl.vroste.zio.kinesis.client.producer.ProducerLive.{
  maxPayloadSizePerRequest,
  maxRecordsPerRequest,
  payloadSizeForEntry,
  ProduceRequest
}
import zio.Chunk

final case class PutRecordsBatch(entries: Chunk[ProduceRequest], nrRecords: Int, payloadSize: Long) {
  def add(entry: ProduceRequest): PutRecordsBatch =
    copy(
      entries = entries :+ entry,
      nrRecords = nrRecords + 1,
      payloadSize = payloadSize + payloadSizeForEntry(entry.r)
    )

  lazy val entriesInOrder: Chunk[ProduceRequest] = entries.sortBy(e => -1 * e.attemptNumber)

  def isWithinLimits =
    nrRecords <= maxRecordsPerRequest &&
      payloadSize <= maxPayloadSizePerRequest
}

object PutRecordsBatch {
  val empty = PutRecordsBatch(Chunk.empty, 0, 0)
}
