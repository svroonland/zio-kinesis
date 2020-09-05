package nl.vroste.zio.kinesis.client.examples

import nl.vroste.zio.kinesis.client
import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.{ Client, Producer }
import zio._
import zio.console.putStrLn
import zio.logging.slf4j.Slf4jLogger

object ProducerExample extends zio.App {
  val streamName      = "my_stream"
  val applicationName = "my_awesome_zio_application"

  val loggingLayer = Slf4jLogger.make((_, logEntry) => logEntry, Some(getClass.getName))
  val env          = client.defaultAwsLayer >+> Client.live ++ loggingLayer

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
