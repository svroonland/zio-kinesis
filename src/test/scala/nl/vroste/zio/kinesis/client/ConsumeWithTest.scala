package nl.vroste.zio.kinesis.client

import java.util.UUID

import nl.vroste.zio.kinesis.client.DynamicConsumer.consumeWith
import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.serde.Serde
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console._
import zio.duration._
import zio.logging.Logging
import zio.stream.ZStream
import zio.test.TestAspect._
import zio.test._

object ConsumeWithTest extends DefaultRunnableSpec {
  import TestUtil._

  private val loggingLayer: ZLayer[Console with Clock, Nothing, Logging] =
    Logging.console(
      format = (_, logEntry) => logEntry,
      rootLoggerName = Some("default-logger")
    )

  private val env: ZLayer[Any with Console with Clock, Throwable, Client with AdminClient with Has[
    DynamicConsumer.Service
  ] with Clock with Logging] =
    (LocalStackServices.localHttpClient >>> LocalStackServices.kinesisAsyncClientLayer >>> (Client.live ++ AdminClient.live ++ LocalStackServices.dynamicConsumerLayer)) ++ Clock.live ++ loggingLayer

  def testConsume1 =
    testM("consume records produced on all shards produced on the stream") {
      val streamName      = "zio-test-stream-" + UUID.randomUUID().toString
      val applicationName = "zio-test-" + UUID.randomUUID().toString
      val nrRecords       = 4

      for {
        assert <- createStream(streamName, 2)
                  //        .provideSomeLayer[Console](env)
                  .use { _ =>
                    (for {
                      _                 <- putStrLn("Putting records")
                      _                 <- ZIO
                             .accessM[Client](
                               _.get
                                 .putRecords(
                                   streamName,
                                   Serde.asciiString,
                                   Seq(ProducerRecord("key1", "msg1"), ProducerRecord("key2", "msg2"))
                                 )
                             )
                             .tapError(e => putStrLn(s"error1: $e").provideLayer(Console.live))
                             .retry(retryOnResourceNotFound)
                      refProcessed      <- Ref.make(Seq.empty[String])
                      finishedConsuming <- Promise.make[Nothing, Unit]
                      _                  = println(s"fakeRecordProcessor $refProcessed $finishedConsuming")
                      _                 <- putStrLn("Starting dynamic consumer")
                      _                 <- consumeWith(
                             streamName,
                             applicationName = applicationName,
                             deserializer = Serde.asciiString,
                             isEnhancedFanOut = false
                           )(r => putStrLn(s"XXXXXXXXXXXX $r").provideLayer(Console.live))

                    } yield assertCompletes)
                  // TODO this assertion doesn't do what the test says
                  }
      } yield assert

    }

  override def spec =
    suite("ConsumeWithTest")(
      testConsume1
    ).provideCustomLayerShared(env.orDie) @@ timeout(30.seconds) @@ sequential

  def delayStream[R, E, O](s: ZStream[R, E, O], delay: Duration) =
    ZStream.fromEffect(ZIO.sleep(delay)).flatMap(_ => s)

  def awaitRefPredicate[T](ref: Ref[T])(predicate: T => Boolean) =
    (for {
      p <- Promise.make[Nothing, Unit]
      _ <- ZIO
             .whenM(ref.get.map(predicate))(p.succeed(()))
             .repeat(Schedule.fixed(1.second))
             .fork
      _ <- p.await
    } yield ()).fork

}
