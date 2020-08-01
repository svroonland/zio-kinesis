package nl.vroste.zio.kinesis.client

import java.util.UUID

import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.DynamicConsumer.consumeWith
import nl.vroste.zio.kinesis.client.serde.Serde
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console._
import zio.duration._
import zio.logging.Logging
import zio.test.TestAspect._
import zio.test._

object ConsumeWithTest extends DefaultRunnableSpec {
  import TestUtil._

  val nrRecords = 4

  private val loggingLayer: ZLayer[Any, Nothing, Logging] =
    Console.live ++ Clock.live >>> Logging.console(
      format = (_, logEntry) => logEntry,
      rootLoggerName = Some("default-logger")
    )

  private val env =
    (LocalStackServices.localHttpClient >>> LocalStackServices.kinesisAsyncClientLayer >>> (Client.live ++ AdminClient.live ++ LocalStackServices.dynamicConsumerLayer)) ++ Clock.live ++ Blocking.live ++ loggingLayer

  def testConsume1 =
    testM("consume records produced on all shards produced on the stream") {
      val streamName      = "zio-test-stream-" + UUID.randomUUID().toString
      val applicationName = "zio-test-" + UUID.randomUUID().toString

      for {
        refProcessed      <- Ref.make(Seq.empty[String])
        finishedConsuming <- Promise.make[Nothing, Unit]
        assert            <- createStream(streamName, 2).use { _ =>
                    (for {
                      _             <- putStrLn("Putting records")
                      _             <- ZIO
                             .accessM[Client](
                               _.get
                                 .putRecords(
                                   streamName,
                                   Serde.asciiString,
                                   Seq(
                                     ProducerRecord("key1", "msg1"),
                                     ProducerRecord("key2", "msg2"),
                                     ProducerRecord("key3", "msg3"),
                                     ProducerRecord("key4", "msg4")
                                   )
                                 )
                             )
                             .tapError(e => putStrLn(s"error1: $e").provideLayer(Console.live))
                             .retry(retryOnResourceNotFound)
                      _              = println(s"fakeRecordProcessor $refProcessed $finishedConsuming")
                      _             <- putStrLn("Starting dynamic consumer")
                      consumerFiber <- consumeWith[Any, String](
                                         streamName,
                                         applicationName = applicationName,
                                         deserializer = Serde.asciiString,
                                         isEnhancedFanOut = false,
                                         batchSize = 2
                                       ) {
                                         val x: Record[String] => ZIO[Any, Throwable, Unit] = TestRecordProcessor
                                           .process(
                                             refProcessed,
                                             finishedConsuming,
                                             expectedCountOrFailFunction = Right(nrRecords)
                                           )
                                         x
                                       }.fork
                      _             <- finishedConsuming.await
                      _             <- consumerFiber.interrupt
                    } yield assertCompletes)
                  // TODO this assertion doesn't do what the test says
                  }
      } yield assert

    }

  override def spec =
    suite("ConsumeWithTest")(
      testConsume1
    ).provideCustomLayer(env.orDie) @@ timeout(60.seconds) @@ sequential

}

object TestRecordProcessor {
  import zio.logging.Logging
  import zio.logging.log._

  private val loggingLayer: ZLayer[Any, Nothing, Logging] =
    Console.live ++ Clock.live >>> Logging.console(
      format = (_, logEntry) => logEntry,
      rootLoggerName = Some("default-logger")
    )

  def process[T](
    refProcessed: Ref[Seq[T]],
    promise: Promise[Nothing, Unit],
    expectedCountOrFailFunction: Either[T => Boolean, Int]
  ): Record[T] => ZIO[Any, Throwable, Unit] =
    rec =>
      {
        val data = rec.data

        def error(rec: T) = new IllegalStateException(s"Failed processing record " + rec)

        val updateRefProcessed =
          for {
            _       <- refProcessed.update(xs => xs :+ data)
            refPost <- refProcessed.get
            sizePost = refPost.distinct.size
            _       <- info(s"process records count after ${refPost.size} rec = $data")
          } yield sizePost

        for {
          refPre <- refProcessed.get
          _      <- info(s"process records count before ${refPre.size} before. rec = $data")
          _      <- expectedCountOrFailFunction.fold(
                 failFunction =>
                   if (failFunction(data))
                     info(s"record $data, about to return error") *> Task.fail(error(data))
                   else
                     updateRefProcessed,
                 expectedCount =>
                   for {
                     sizePost <- updateRefProcessed
                     _        <- info(s"XXXXXXXXXXXXXX processed $sizePost, expected $expectedCount")
                     _        <- ZIO.when(sizePost == expectedCount)(
                            info(s"about to call promise.succeed on processed count $sizePost") *> promise.succeed(())
                          )
                   } yield ()
               )
        } yield ()

      }.provideLayer(loggingLayer)
}
