package nl.vroste.zio.kinesis.client

import zio._

/**
 * Can be used for both native `Consumer.consumeWith` and `DynamicConsumer.consumeWith` to create a fake record
 * processor that can be used to test record processing against accumulated state in `refProcessed: Ref[Seq[T]]`
 */
object FakeRecordProcessor {

  /**
   * Creates a fake record processor
   *
   * @param refProcessed
   *   Ref to accumulate processed records
   * @param promise
   *   Promise to signal when `expectedCount`` number of records have been processed
   * @param expectedCount
   * @tparam T
   *   Type of the record
   * @return
   */
  def make[T](
    refProcessed: Ref[Seq[T]],
    promise: Promise[Nothing, Unit],
    expectedCount: Int
  ): T => RIO[Any, Unit] = process(refProcessed, promise, Right(expectedCount))

  /**
   * Creates a fake record processor
   *
   * @param refProcessed
   *   Ref to accumulate processed records
   * @param promise
   *   Promise to signal when `expectedCount`` number of records have been processed
   * @param failFunction
   * @tparam T
   *   Type of the record
   * @return
   */
  def makeFailing[T](
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
