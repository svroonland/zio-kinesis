package nl.vroste.zio.kinesis.client.dynamicconsumer

import zio._

object FakeRecordProcessor {
  def make[T](
    refProcessed: Ref[Seq[T]],
    promise: Promise[Nothing, Unit],
    expectedCount: Int
  ): DynamicConsumer.Record[T] => RIO[Any, Unit] = process(refProcessed, promise, Right(expectedCount))

  def makeFailing[RC, T](
    refProcessed: Ref[Seq[T]],
    promise: Promise[Nothing, Unit],
    failFunction: T => Boolean
  ): DynamicConsumer.Record[T] => RIO[Any, Unit] = process(refProcessed, promise, Left(failFunction))

  private def process[T](
    refProcessed: Ref[Seq[T]],
    promise: Promise[Nothing, Unit],
    failFunctionOrExpectedCount: Either[T => Boolean, Int]
  ): DynamicConsumer.Record[T] => RIO[Any, Unit] =
    rec => {
      val data          = rec.data
      def error(rec: T) = new IllegalStateException(s"Failed processing record " + rec)

      val updateRefProcessed =
        for {
          processed <- refProcessed.updateAndGet(xs => xs :+ data)
          sizeAfter  = processed.distinct.size
          _         <- ZIO.logInfo(s"process records count ${processed.size}, rec = $rec")
        } yield sizeAfter

      for {
        _ <- failFunctionOrExpectedCount.fold(
               failFunction =>
                 if (failFunction(data))
                   ZIO.logWarning(s"record $rec, about to return error") *> Task.fail(error(data))
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
