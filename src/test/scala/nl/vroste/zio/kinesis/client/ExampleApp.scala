package nl.vroste.zio.kinesis.client
import java.util.UUID

import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.TestUtil.retryOnResourceNotFound
import nl.vroste.zio.kinesis.client.serde.Serde
import zio.console._
import zio.duration._
import zio.stream.{ ZStream, ZTransducer }
import zio.{ Chunk, ExitCode, Promise, Schedule, UIO, ZIO, ZManaged }

object ExampleApp extends zio.App {

  def withGracefulShutdown[R, E, A](f: UIO[Unit] => ZIO[R, E, A]) =
    (for {
      requestShutdown <- Promise.make[Nothing, Unit].toManaged_
      streamFiber     <- f(requestShutdown.await).forkManaged
      result          <- Promise.make[E, A].toManaged_
      _               <- ZManaged.unit.ensuring {
             UIO(println("WGS INTERRUPT")) <*
               requestShutdown.succeed(()) *>
                 streamFiber.join.foldM(result.fail, result.succeed)
           }
    } yield (result, requestShutdown)).use {
      case (result, requestShutdown) => result.await.onInterrupt(requestShutdown.succeed(()))
    }

  override def run(
    args: List[String]
  ): ZIO[zio.ZEnv, Nothing, ExitCode] = {

    val streamName = "zio-test-stream-" + UUID.randomUUID().toString

    TestUtil
      .createStreamUnmanaged(streamName, 2) *>
      (for {
        _         <- produceRecords(streamName, 2000).fork
        streamFib <- withGracefulShutdown { requestShutdown =>
                       LocalStackDynamicConsumer
                         .shardedStream(
                           streamName,
                           applicationName = "testApp",
                           deserializer = Serde.asciiString,
                           requestShutdown = requestShutdown
                         )
                         .flatMapPar(Int.MaxValue) {
                           case (shardID, shardStream) =>
                             shardStream
                               .tap(r => putStrLn(s"Got record $r").delay(100.millis))
                               .aggregateAsyncWithin(ZTransducer.last, Schedule.fixed(1.second))
                               .mapConcat(_.toList)
                               .tap { r =>
                                 putStrLn(s"Checkpointing ${shardID}") *> r.checkpoint
                               }
                         }
                         .runCollect
                     }.fork
        _         <- putStrLn("Press enter to exit")
        _         <- ZIO.unit.delay(20.seconds)
        _         <- putStrLn("Exiting app")
//        _         <- streamFib.interrupt
//          _           <- (putStrLn("Interrupting") *> streamFiber.interrupt).delay(15.seconds)
        // TODO we want to be able to catch an external interrupt and handle it correctly (top down)

      } yield ExitCode.success)
  }.orDie

  def produceRecords(streamName: String, nrRecords: Int) =
    (for {
      client   <- Client.build(LocalStackDynamicConsumer.kinesisAsyncClientBuilder)
      producer <- Producer.make(streamName, client, Serde.asciiString)
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
