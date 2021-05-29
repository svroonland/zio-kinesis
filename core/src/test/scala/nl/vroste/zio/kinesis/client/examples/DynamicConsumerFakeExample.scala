package nl.vroste.zio.kinesis.client.examples

import java.nio.ByteBuffer

import nl.vroste.zio.kinesis.client.DynamicConsumer
import nl.vroste.zio.kinesis.client.Record
import nl.vroste.zio.kinesis.client.fake.DynamicConsumerFake
import nl.vroste.zio.kinesis.client.serde.Serde
import zio._
import zio.clock.Clock
import zio.console.{ putStrLn, Console }
import zio.duration._
import zio.logging.Logging
import zio.stream.ZStream

/**
 * Basic usage example for `DynamicConsumerFake`
 */
object DynamicConsumerFakeExample extends zio.App {
  val loggingLayer: ZLayer[Any, Nothing, Logging] =
    (Console.live ++ Clock.live) >>> Logging.console() >>> Logging.withRootLoggerName(getClass.getName)

  private val shards: ZStream[Any, Nothing, (String, ZStream[Any, Throwable, ByteBuffer])] =
    DynamicConsumerFake.shardsFromStreams(Serde.asciiString, ZStream("msg1", "msg2"), ZStream("msg3", "msg4"))

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    for {
      refCheckpointedList <- Ref.make[Seq[Record[Any]]](Seq.empty)
      exitCode            <- DynamicConsumer
                    .consumeWith(
                      streamName = "my-stream",
                      applicationName = "my-application",
                      deserializer = Serde.asciiString,
                      workerIdentifier = "worker1",
                      checkpointBatchSize = 1000L,
                      checkpointDuration = 5.minutes
                    )(record => putStrLn(s"Processing record $record"))
                    .provideCustomLayer(DynamicConsumer.fake(shards, refCheckpointedList) ++ loggingLayer)
                    .exitCode
      _                   <- putStrLn(s"refCheckpointedList=$refCheckpointedList")
    } yield exitCode

}
