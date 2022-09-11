package nl.vroste.zio.kinesis.client.examples

import nl.vroste.zio.kinesis.client
import nl.vroste.zio.kinesis.client.producer.ProducerMetrics
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.{ Producer, ProducerRecord, ProducerSettings }
import zio.Console.printLine
import zio._

object ProducerWithMetricsExample extends ZIOAppDefault {
  val streamName      = "my_stream"
  val applicationName = "my_awesome_zio_application"

  val env = client.defaultAwsLayer ++ Scope.default

  val program = (for {
    totalMetrics <- Ref.make(ProducerMetrics.empty)
    producer     <- Producer
                      .make(
                        streamName,
                        Serde.asciiString,
                        ProducerSettings(),
                        metrics => totalMetrics.updateAndGet(_ + metrics).flatMap(m => printLine(m.toString).orDie)
                      )
  } yield (producer, totalMetrics)).flatMap { case (producer, totalMetrics) =>
    val records = (1 to 100).map(j => ProducerRecord(s"key${j}", s"message${j}"))

    for {
      _ <- producer.produceChunk(Chunk.fromIterable(records))
      _ <- printLine(s"All records in the chunk were produced").orDie
      m <- totalMetrics.get
      _ <- printLine(s"Metrics after producing: ${m}").orDie
    } yield ()
  }

  override def run: ZIO[Any with ZIOAppArgs with Scope, Any, Any] = program.provideLayer(env).exitCode
}
