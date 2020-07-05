package nl.vroste.zio.kinesis.client
import java.time.Instant

import software.amazon.awssdk.services.kinesis.model.EncryptionType

case class Record[T](
  shardId: String,
  sequenceNumber: String,
  approximateArrivalTimestamp: Instant,
  data: T,
  partitionKey: String,
  encryptionType: EncryptionType,
  subSequenceNumber: Long,
  explicitHashKey: String,
  aggregated: Boolean
)
