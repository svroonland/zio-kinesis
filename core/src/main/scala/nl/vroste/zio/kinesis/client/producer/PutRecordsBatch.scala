package nl.vroste.zio.kinesis.client.producer
import nl.vroste.zio.kinesis.client.producer.ProducerLive.{
  maxPayloadSizePerRequest,
  maxRecordsPerRequest,
  payloadSizeForEntry,
  ProduceRequest
}

final case class PutRecordsBatch(entries: List[ProduceRequest], nrRecords: Int, payloadSize: Long) {
  def add(entry: ProduceRequest): PutRecordsBatch =
    copy(
      entries = entry +: entries,
      nrRecords = nrRecords + 1,
      payloadSize = payloadSize + payloadSizeForEntry(entry.r)
    )

  lazy val entriesInOrder: Seq[ProduceRequest] = entries.reverse.sortBy(e => -1 * e.attemptNumber)

  def isWithinLimits =
    nrRecords <= maxRecordsPerRequest &&
      payloadSize <= maxPayloadSizePerRequest
}

object PutRecordsBatch {
  val empty = PutRecordsBatch(List.empty, 0, 0)
}
