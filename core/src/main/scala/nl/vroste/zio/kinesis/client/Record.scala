package nl.vroste.zio.kinesis.client
import java.time.Instant

import zio.aws.kinesis.model.EncryptionType

final case class Record[+T](
  shardId: String,
  sequenceNumber: String,
  approximateArrivalTimestamp: Instant,
  data: T,
  partitionKey: String,
  encryptionType: Option[EncryptionType],
  subSequenceNumber: Option[Long],
  explicitHashKey: Option[String],
  aggregated: Boolean
)
