package nl.vroste.zio.kinesis.client.examples

import nl.vroste.zio.kinesis.client
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.{ Producer, ProducerMetrics, ProducerRecord, ProducerSettings }
import zio._
import zio.console.putStrLn
import zio.logging.slf4j.Slf4jLogger

object ProducerWithMetricsExample extends zio.App {
  val streamName      = "my_stream"
  val applicationName = "my_awesome_zio_application"

  val loggingLayer = Slf4jLogger.make((_, logEntry) => logEntry, Some(getClass.getName))
  val env          = client.defaultEnvironment ++ loggingLayer

  val program = (for {
    totalMetrics <- Ref.make(ProducerMetrics.empty).toManaged_
    producer     <- Producer
                  .make(
                    streamName,
                    Serde.asciiString,
                    ProducerSettings(),
                    metrics => totalMetrics.updateAndGet(_ + metrics).flatMap(m => putStrLn(m.toString))
                  )
  } yield (producer, totalMetrics)).use {
    case (producer, totalMetrics) =>
      val records = (1 to 100).map(j => ProducerRecord(s"key${j}", s"message${j}"))

      for {
        _ <- producer.produceChunk(Chunk.fromIterable(records))
        _ <- putStrLn(s"All records in the chunk were produced")
        m <- totalMetrics.get
        _ <- putStrLn(s"Metrics after producing: ${m}")
      } yield ()
  }

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    program.provideCustomLayer(env).exitCode
}
