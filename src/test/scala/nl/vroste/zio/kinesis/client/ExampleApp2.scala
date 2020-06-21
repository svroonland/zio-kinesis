package nl.vroste.zio.kinesis.client

import java.util.UUID

import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.TestUtil.retryOnResourceNotFound
import nl.vroste.zio.kinesis.client.serde.Serde
import software.amazon.kinesis.exceptions.ShutdownException
import zio._
import zio.console._
import zio.duration._
import zio.stream.{ ZStream, ZTransducer }

/*
 * Example application that requests an orderly stream shutdown from the outside.
 */
object ExampleApp2 extends zio.App {
  private val clientLayer = LocalStackLayers.kinesisAsyncClientLayer >>> ClientLive.layer

  override def run(
    args: List[String]
  ): ZIO[zio.ZEnv, Nothing, ExitCode] = {

    val streamName = "zio-test-stream-" + UUID.randomUUID().toString

    for {
      _           <- TestUtil.createStreamUnmanaged(streamName, 10)
      _           <- produceRecords(streamName, 20000).fork
      interrupted <- Promise.make[Nothing, Unit]
      _           <- (for {
               service <- ZStream.service[DynamicConsumer.Service]
               stream  <- service
                           .shardedStream(
                             streamName,
                             applicationName = "testApp-" + UUID.randomUUID().toString(),
                             deserializer = Serde.asciiString,
                             isEnhancedFanOut = false,
                             requestShutdown = interrupted.await.tap(_ => UIO(println("Interrupting shardedStream")))
                           )
                           .flatMapPar(Int.MaxValue) {
                             case (shardID, shardStream, checkpointer) =>
                               shardStream
                                 .tap(r =>
                                   checkpointer.stageOnSuccess(putStrLn(s"Processing record $r"))(r)
                                 ) // .delay(100.millis))
                                 .aggregateAsyncWithin(ZTransducer.last, Schedule.fixed(1.second))
                                 .mapConcat(_.toList)
                                 .tap { _ =>
                                   putStrLn(s"Checkpointing ${shardID}") *> checkpointer.checkpoint
                                 }
                                 .catchSome {
                                   // This happens when the lease for the shard is lost. Best we can do is end the stream.
                                   case _: ShutdownException =>
                                     ZStream
                                       .fromEffect(putStr("ShutdownException").provideLayer(Console.live))
                                       .flatMap(_ => ZStream.empty)
                                 }
                           }
             } yield stream).runCollect.fork
      _           <- putStrLn("App started")
      _           <- ZIO.unit.delay(15.seconds) *> interrupted.succeed(())
      _           <- ZIO.unit.delay(10.seconds) // wait for normal KCL shutdown to complete
      _           <- putStrLn("Exiting app")

    } yield ExitCode.success
  }.provideCustomLayer(clientLayer ++ LocalStackLayers.dynamicConsumerLayer).orDie

  def produceRecords(streamName: String, nrRecords: Int) =
    (for {
      producer <- Producer.make(streamName, Serde.asciiString)
    } yield producer).use { producer =>
      val records =
        (1 to nrRecords).map(i => ProducerRecord(s"key$i", s"msg$i"))
      ZStream
        .fromIterable(records)
        .chunkN(10)
        .mapChunksM(
          producer
            .produceChunk(_)
            .tapError(e => putStrLn(s"error: $e").provideLayer(Console.live))
            .retry(retryOnResourceNotFound)
            .as(Chunk.unit)
            .delay(1.second)
        )
        .runDrain
    }
}
