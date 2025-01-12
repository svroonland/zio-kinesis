package nl.vroste.zio.kinesis.client

import zio._

object FakeRecordProcessor {
  def make[T](
    refProcessed: Ref[Seq[T]],
    promise: Promise[Nothing, Unit],
    expectedCount: Int
  ): T => RIO[Any, Unit] = process(refProcessed, promise, Right(expectedCount))

  def makeFailing[RC, T](
    refProcessed: Ref[Seq[T]],
    promise: Promise[Nothing, Unit],
    failFunction: T => Boolean
  ): T => RIO[Any, Unit] = process(refProcessed, promise, Left(failFunction))

  private def process[T](
    refProcessed: Ref[Seq[T]],
    promise: Promise[Nothing, Unit],
    failFunctionOrExpectedCount: Either[T => Boolean, Int]
  ): T => RIO[Any, Unit] =
    data => {
      def error(data: T) = new IllegalStateException(s"Failed processing record " + data)

      val updateRefProcessed =
        for {
          processed <- refProcessed.updateAndGet(xs => xs :+ data)
          sizeAfter  = processed.distinct.size
          _         <- ZIO.logInfo(s"process records count ${processed.size}, rec = $data")
        } yield sizeAfter

      for {
        _ <- failFunctionOrExpectedCount.fold(
               failFunction =>
                 if (failFunction(data))
                   ZIO.logWarning(s"record $data, about to return error") *> ZIO.fail(error(data))
                 else
                   updateRefProcessed,
               expectedCount =>
                 for {
                   sizeAfter <- updateRefProcessed
                   _         <- ZIO.logInfo(s"processed $sizeAfter, expected $expectedCount")
                   _         <-
                     ZIO.when(sizeAfter == expectedCount)(
                       ZIO.logInfo(s"about to call promise.succeed on processed count $sizeAfter") *> promise.succeed(
                         ()
                       )
                     )
                 } yield ()
             )
      } yield ()

    }
}
