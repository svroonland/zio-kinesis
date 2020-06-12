package nl.vroste.zio.kinesis.client

import java.util.UUID

import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.serde.Serde
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console._
import zio.duration._
import zio.stream.ZStream
import zio.test.TestAspect._
import zio.test._

object DynamicConsumerTest2 extends DefaultRunnableSpec {
  import TestUtil._

  def testConsume1 =
    testM("consume records produced on all shards produced on the stream") {
      val streamName      = "zio-test-stream-" + UUID.randomUUID().toString
      val applicationName = "zio-test-" + UUID.randomUUID().toString

      (Client.build(LocalStackDynamicConsumer.kinesisAsyncClientBuilder) <* createStream(streamName, 2)).use { client =>
        (for {
          _ <- putStrLn("Putting records")
          _ <- client
                 .putRecords(
                   streamName,
                   Serde.asciiString,
                   Seq(ProducerRecord("key1", "msg1"), ProducerRecord("key2", "msg2"))
                 )
                 .retry(retryOnResourceNotFound)
                 .provideLayer(Clock.live)

          _ <- putStrLn("Starting dynamic consumer")
          _ <- (for {
                   service <- ZIO
                                .service[DynamicConsumer2.Service]
                   _       <- service
                          .shardedStream(
                            streamName,
                            applicationName = applicationName,
                            deserializer = Serde.asciiString,
                            isEnhancedFanOut = false
                          )
                          .flatMapPar(Int.MaxValue) {
                            case (shardId @ _, shardStream, checkpointer) =>
                              shardStream.tap(r =>
                                putStrLn(s"Got record $r") *> checkpointer
                                  .checkpointNow(r)
                                  .retry(Schedule.exponential(100.millis))
                              )
                          }
                          .take(2)
                          .runCollect
                 } yield ()).provideSomeLayer[Clock with Blocking with Console](
                 LocalStackDynamicConsumer2.localstackDynamicConsumerLayer
               )

        } yield assertCompletes)
      // TODO this assertion doesn't do what the test says
      }
    }

  override def spec =
    suite("DynamicConsumer2")(
      testConsume1
    ) @@ timeout(30.seconds) @@ sequential

  def sleep(d: Duration) = ZIO.sleep(d).provideLayer(Clock.live)

  def delayStream[R, E, O](s: ZStream[R, E, O], delay: Duration) =
    ZStream.fromEffect(sleep(delay)).flatMap(_ => s)

  def awaitRefPredicate[T](ref: Ref[T])(predicate: T => Boolean) =
    (for {
      p <- Promise.make[Nothing, Unit]
      _ <- ZIO
             .whenM(ref.get.map(predicate))(p.succeed(()))
             .repeat(Schedule.fixed(1.second))
             .provideLayer(Clock.live)
             .fork
      _ <- p.await
    } yield ()).fork

}
