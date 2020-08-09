package nl.vroste.zio.kinesis.client

import java.util.concurrent.{ CompletableFuture, CompletionException }

import software.amazon.awssdk.core.exception.SdkException
import zio.clock.Clock
import zio.duration._
import zio.stream.ZStream
import zio._

object Util {
  implicit class ZStreamExtensions[-R, +E, +O](val stream: ZStream[R, E, O]) extends AnyVal {

    /**
     * When the stream fails, retry it according to the given schedule
     *
     * This retries the entire stream, so will re-execute all of the stream's acquire operations.
     *
     * The schedule is reset as soon as the first element passes through the stream again.
     *
      * @param schedule Schedule receiving as input the errors of the stream
     * @return Stream outputting elements of all attempts of the stream
     */
    // To be included in ZIO with https://github.com/zio/zio/pull/3902
    def retry[R1 <: R](schedule: Schedule[R1, E, _]): ZStream[R1 with Clock, E, O] =
      ZStream {
        for {
          driver       <- schedule.driver.toManaged_
          currStream   <- Ref.make[ZIO[R, Option[E], Chunk[O]]](IO.fail(None)).toManaged_ // IO.fail(None) = Pull.end
          switchStream <- ZManaged.switchable[R, Nothing, ZIO[R, Option[E], Chunk[O]]]
          _            <- switchStream(stream.process).flatMap(currStream.set).toManaged_
          pull          = {
            def loop: ZIO[R1 with Clock, Option[E], Chunk[O]] =
              currStream.get.flatten.catchSome {
                case Some(e) =>
                  driver
                    .next(e)
                    .foldM(
                      // Failure of the schedule indicates it doesn't accept the input
                      _ => IO.fail(Some(e)), // TODO = Pull.fail
                      _ =>
                        switchStream(stream.process).flatMap(currStream.set) *>
                          // Reset the schedule when a chunk is successfully pulled
                          loop.tap(_ => driver.reset)
                    )
              }

            loop
          }
        } yield pull
      }
  }

  def asZIO[T](f: => CompletableFuture[T]): Task[T] =
    ZIO
      .effect(f)
      .flatMap(ZIO.fromCompletionStage(_))
      .catchSome {
        case e: CompletionException =>
          ZIO.fail(Option(e.getCause()).getOrElse(e))
        case e: SdkException        =>
          ZIO.fail(Option(e.getCause()).getOrElse(e))
      }

  // TODO there might be some optimization here
  def paginatedRequest[R, E, A, Token](fetch: Option[Token] => ZIO[R, E, (A, Option[Token])])(
    throttling: Schedule[Clock, Any, Long] = Schedule.forever
  ): ZStream[Clock with R, E, A] =
    ZStream.fromEffect(fetch(None)).flatMap {
      case (results, nextTokenOpt) =>
        ZStream.succeed(results) ++ (nextTokenOpt match {
          case None            => ZStream.empty
          case Some(nextToken) =>
            ZStream.paginateM[R, E, A, Token](nextToken)(token => fetch(Some(token))).repeatElements(throttling)
        })
    }

  /**
   * Schedule for exponential backoff up to a maximum interval and an optional maximum number of retries
   *
   * @param min Minimum backoff time
   * @param max Maximum backoff time. When this value is reached, subsequent intervals will be equal to this value.
   * @param factor Exponential factor. 2 means doubling, 1 is constant, < 1 means decreasing
   * @param maxRecurs Maximum retries. When this number is exceeded, the schedule will end
   * @tparam A Schedule input
   */
  def exponentialBackoff[A](
    min: Duration,
    max: Duration,
    factor: Double = 2.0,
    maxRecurs: Option[Int] = None
  ): Schedule[Clock, A, (Duration, Long)] =
    (Schedule.exponential(min, factor).whileOutput(_ <= max) andThen Schedule.fixed(max).as(max)) &&
      maxRecurs.map(Schedule.recurs).getOrElse(Schedule.forever)

  /**
   * Executes calls through a token bucket stream, ensuring a maximum rate of calls
   *
   * Allows for bursting
   *
   * @param units Maximum number of calls per duration
   * @param duration Duration for nr of tokens
   * @return The original function with rate limiting applied, as a managed resource
   */

  def throttledFunction[R, I, E, A](units: Int, duration: Duration)(
    f: I => ZIO[R, E, A]
  ): ZManaged[Clock, Nothing, I => ZIO[R, E, A]] =
    for {
      requestsQueue <- Queue.bounded[(IO[E, A], Promise[E, A])](units / 2 * 2).toManaged_
      _             <- ZStream
             .fromQueueWithShutdown(requestsQueue)
             .throttleShape(units.toLong, duration, units.toLong)(_ => 1)
             .mapM { case (effect, promise) => promise.completeWith(effect) }
             .runDrain
             .forkManaged
    } yield (input: I) =>
      for {
        env     <- ZIO.environment[R]
        promise <- Promise.make[E, A]
        _       <- requestsQueue.offer((f(input).provide(env), promise))
        result  <- promise.await
      } yield result

  def throttledFunctionN(units: Int, duration: Duration): ThrottledFunctionPartial =
    ThrottledFunctionPartial(units, duration)

  final case class ThrottledFunctionPartial(units: Int, duration: Duration) {
    def apply[R, I0, I1, E, A](f: (I0, I1) => ZIO[R, E, A]): ZManaged[Clock, Nothing, (I0, I1) => ZIO[R, E, A]] =
      throttledFunction[R, (I0, I1), E, A](units, duration) {
        case (i0, i1) => f(i0, i1)
      }.map(Function.untupled(_))

    def apply[R, I0, I1, I2, E, A](
      f: (I0, I1, I2) => ZIO[R, E, A]
    ): ZManaged[Clock, Nothing, (I0, I1, I2) => ZIO[R, E, A]] =
      throttledFunction[R, (I0, I1, I2), E, A](units, duration) {
        case (i0, i1, i2) => f(i0, i1, i2)
      }.map(Function.untupled(_))
  }
}
