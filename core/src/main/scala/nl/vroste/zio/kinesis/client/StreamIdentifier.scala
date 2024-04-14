package nl.vroste.zio.kinesis.client

import zio.aws.kinesis.model.primitives.{ StreamARN, StreamName }

sealed trait StreamIdentifier {
  def name: Option[StreamName]
  def arn: Option[StreamARN]
}

object StreamIdentifier {
  case class StreamIdentifierByName(streamName: StreamName) extends StreamIdentifier {
    override def name: Option[StreamName] = Some(streamName)

    override def arn: Option[StreamARN] = None
  }
  case class StreamIdentifierByArn(streamARN: StreamARN)    extends StreamIdentifier {
    override def name: Option[StreamName] = None

    override def arn: Option[StreamARN] = Some(streamARN)
  }

  def apply(streamIdentifier: String): StreamIdentifier =
    if (streamIdentifier.startsWith("arn:aws:kinesis")) StreamIdentifierByArn(StreamARN(streamIdentifier))
    else StreamIdentifierByName(StreamName(streamIdentifier))

  def fromName(streamName: StreamName) = StreamIdentifierByName(streamName)
  def fromARN(streamARN: StreamARN)    = StreamIdentifierByArn(streamARN)

  implicit def stringIsStreamIdentifier(stream: String): StreamIdentifier = StreamIdentifier(stream)

}
