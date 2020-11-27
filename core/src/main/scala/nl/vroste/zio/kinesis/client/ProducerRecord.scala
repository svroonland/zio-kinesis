package nl.vroste.zio.kinesis.client

final case class ProducerRecord[T](partitionKey: String, data: T)
