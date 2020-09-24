package nl.vroste.zio.kinesis.client.examples

import nl.vroste.zio.kinesis.client
import nl.vroste.zio.kinesis.client.producer.ProducerMetrics
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.{ Producer, ProducerRecord, ProducerSettings }
import zio._
import zio.clock.Clock
import zio.console.{ putStrLn, Console }
import zio.logging.Logging

object ProducerWithMetricsExample extends zio.App {
  val streamName      = "my_stream"
  val applicationName = "my_awesome_zio_application"

  val loggingLayer: ZLayer[Any, Nothing, Logging] =
    (Console.live ++ Clock.live) >>> Logging.console() >>> Logging.withRootLoggerName(getClass.getName)

  val env = client.defaultEnvironment ++ loggingLayer

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
