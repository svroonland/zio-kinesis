package nl.vroste.zio.kinesis.client.examples

import java.nio.ByteBuffer

import nl.vroste.zio.kinesis.client.{ DynamicConsumer, _ }
import nl.vroste.zio.kinesis.client.serde.Serde
import zio._
import zio.console.putStrLn
import zio.duration._
import zio.logging.slf4j.Slf4jLogger
import zio.stream.ZStream

/**
 * Basic usage example for `DynamicConsumerFake`
 */
object FakeDynamicConsumerExample extends zio.App {
  private val loggingLayer = Slf4jLogger.make((_, logEntry) => logEntry, Some(getClass.getName))

  val shard: UIO[(String, ZStream[Any, Throwable, ByteBuffer])]                    =
    DynamicConsumerFake.makeShard(Serde.asciiString, "testShard", List("msg1", "msg2"))
  val shards: ZStream[Any, Nothing, (String, ZStream[Any, Throwable, ByteBuffer])] = ZStream.fromEffect(shard)

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    for {
      refCheckpointedList <- Ref.make[Seq[Any]](Seq.empty[String])
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
