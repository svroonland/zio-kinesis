package nl.vroste.zio.kinesis.client

object Client {

  final case class ProducerRecord[T](partitionKey: String, data: T)
}
