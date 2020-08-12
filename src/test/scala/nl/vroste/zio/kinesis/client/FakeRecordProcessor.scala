package nl.vroste.zio.kinesis.client

import nl.vroste.zio.kinesis.client.DynamicConsumer.RecordProcessor
import zio._

object FakeRecordProcessor {
  import zio.logging.Logging
  import zio.logging.log._

  def make[T](
    refProcessed: Ref[Seq[T]],
    promise: Promise[Nothing, Unit],
    expectedCount: Int
  ): RecordProcessor[Logging, T] = RecordProcessor(process(refProcessed, promise, Right(expectedCount)))

  def makeFailing[T](
    refProcessed: Ref[Seq[T]],
    promise: Promise[Nothing, Unit],
    failFunction: T => Boolean
  ): RecordProcessor[Logging, T] = RecordProcessor(process(refProcessed, promise, Left(failFunction)))

  private def process[T](
    refProcessed: Ref[Seq[T]],
    promise: Promise[Nothing, Unit],
    failFunctionOrExpectedCount: Either[T => Boolean, Int]
  ): Record[T] => RIO[Logging, Unit] =
    rec => {
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

    }
}
