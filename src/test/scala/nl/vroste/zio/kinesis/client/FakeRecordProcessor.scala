package nl.vroste.zio.kinesis.client

import zio.clock.Clock
import zio.console.Console
import zio._

object FakeRecordProcessor {
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
                     _        <- info(s"processed $sizePost, expected $expectedCount")
                     _        <- ZIO.when(sizePost == expectedCount)(
                            info(s"about to call promise.succeed on processed count $sizePost") *> promise.succeed(())
                          )
                   } yield ()
               )
        } yield ()

      }.provideLayer(loggingLayer)
}
