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

  def make[T](
    refProcessed: Ref[Seq[T]],
    promise: Promise[Nothing, Unit],
    expectedCount: Int
  ): Record[T] => ZIO[Any, Throwable, Unit] = process(refProcessed, promise, Right(expectedCount))

  def makeFailing[T](
    refProcessed: Ref[Seq[T]],
    promise: Promise[Nothing, Unit],
    failFunction: T => Boolean
  ): Record[T] => ZIO[Any, Throwable, Unit] = process(refProcessed, promise, Left(failFunction))

  private def process[T](
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
            _         <- info(s"process records count ${processed.size}, rec = $rec")
          } yield sizeAfter

        for {
          _ <- failFunctionOrExpectedCount.fold(
                 failFunction =>
                   if (failFunction(data))
                     warn(s"record $rec, about to return error") *> Task.fail(error(data))
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
