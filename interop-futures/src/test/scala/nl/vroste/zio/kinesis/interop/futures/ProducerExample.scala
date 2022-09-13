package nl.vroste.zio.kinesis.interop.futures

import nl.vroste.zio.kinesis.client.ProducerRecord
import nl.vroste.zio.kinesis.client.serde.Serde

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }

object ProducerExample extends App {
  val producer = Producer.make[String]("my-stream", Serde.asciiString, metricsCollector = m => println(m))

  val done = Future.traverse(List(1 to 10)) { i =>
    producer.produce(ProducerRecord("key1", s"msg${i}"))
  }

  Await.result(done, 30.seconds)

  producer.close()
}
