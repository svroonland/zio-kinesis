package nl.vroste.zio.kinesis.client
import zio.aws.kinesis.model.EncryptionType

import java.time.Instant

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
