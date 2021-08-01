package nl.vroste.zio.kinesis.client.dynamicconsumer

private[dynamicconsumer] final case class ExtendedSequenceNumber(
  sequenceNumber: String,
  subSequenceNumber: Option[Long]
)
