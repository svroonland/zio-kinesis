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
    failFunctionOrExpectedCount: Either[T => Boolean, Int]
  ): Record[T] => ZIO[Any, Throwable, Unit] =
    rec =>
      {
        val data          = rec.data
        def error(rec: T) = new IllegalStateException(s"Failed processing record " + rec)

        val updateRefProcessed =
          for {
            processed <- refProcessed.updateAndGet(xs => xs :+ data)
            sizeAfter  = processed.distinct.size
            _         <- info(s"process records count ${processed.size} rec = $data")
          } yield sizeAfter

        for {
          _ <- failFunctionOrExpectedCount.fold(
                 failFunction =>
                   if (failFunction(data))
                     info(s"record $data, about to return error") *> Task.fail(error(data))
                   else
                     updateRefProcessed,
                 expectedCount =>
                   for {
                     sizeAfter <- updateRefProcessed
                     _         <- info(s"processed $sizeAfter, expected $expectedCount")
                     _         <- ZIO.when(sizeAfter == expectedCount)(
                            info(s"about to call promise.succeed on processed count $sizeAfter") *> promise.succeed(())
                          )
                   } yield ()
               )
        } yield ()

      }.provideLayer(loggingLayer)
}
