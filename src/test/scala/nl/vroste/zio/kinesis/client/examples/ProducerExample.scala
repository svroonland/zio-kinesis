package nl.vroste.zio.kinesis.client.examples

import nl.vroste.zio.kinesis.client
import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.{ Client, Producer }
import zio._
import zio.clock.Clock
import zio.console.{ putStrLn, Console }
import zio.logging.Logging

object ProducerExample extends zio.App {
  val streamName      = "my_stream"
  val applicationName = "my_awesome_zio_application"

  val loggingLayer: ZLayer[Any, Nothing, Logging] =
    (Console.live ++ Clock.live) >>> Logging.console() >>> Logging.withRootLoggerName(getClass.getName)

  val env = client.defaultAwsLayer >+> Client.live ++ loggingLayer

  val program = Producer.make(streamName, Serde.asciiString).use { producer =>
    val record = ProducerRecord("key1", "message1")

    for {
      _ <- producer.produce(record)
      _ <- putStrLn(s"All records in the chunk were produced")
    } yield ()
  }

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    program.provideCustomLayer(env).exitCode
}
