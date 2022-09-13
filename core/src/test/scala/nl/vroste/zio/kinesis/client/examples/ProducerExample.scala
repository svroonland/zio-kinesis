package nl.vroste.zio.kinesis.client.examples

import nl.vroste.zio.kinesis.client
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.{ Producer, ProducerRecord }
import zio.Console.printLine
import zio._

object ProducerExample extends ZIOAppDefault {
  val streamName      = "my_stream"
  val applicationName = "my_awesome_zio_application"

  val env = client.defaultAwsLayer ++ Scope.default

  val program = Producer.make(streamName, Serde.asciiString).flatMap { producer =>
    val record = ProducerRecord("key1", "message1")

    for {
      _ <- producer.produce(record)
      _ <- printLine(s"All records in the chunk were produced")
    } yield ()
  }

  override def run: ZIO[ZIOAppArgs with Scope, Any, Any] =
    program.provideLayer(env).exitCode
}
