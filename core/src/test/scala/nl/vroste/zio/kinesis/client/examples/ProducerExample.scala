package nl.vroste.zio.kinesis.client.examples

import nl.vroste.zio.kinesis.client
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.{ Producer, ProducerRecord }
import zio._
import zio.Console.printLine

object ProducerExample extends zio.ZIOAppDefault {
  val streamName      = "my_stream"
  val applicationName = "my_awesome_zio_application"

  val env = client.defaultAwsLayer

  val program = Producer.make(streamName, Serde.asciiString).use { producer =>
    val record = ProducerRecord("key1", "message1")

    for {
      _ <- producer.produce(record)
      _ <- printLine(s"All records in the chunk were produced")
    } yield ()
  }

  override def run: ZIO[zio.ZEnv with ZIOAppArgs, Any, Any] =
    program.provideCustomLayer(env).exitCode
}
