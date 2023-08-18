package nl.vroste.zio.kinesis.client

import zio._
import zio.stream.ZStream
import zio.stream.ZSink

object Util {
  implicit class ZStreamExtensions[-R, +E, +O](val stream: ZStream[R, E, O]) extends AnyVal {

    def aggregateAsyncWithinDuration[R1 <: R, E1 >: E, A >: O, B](
      aggregator: ZSink[R1, E1, A, A, B],
      timeout: Option[Duration] = None
    ): ZStream[R1, E1, B] =
      timeout.fold(stream.aggregateAsync(aggregator))(timeout =>
        stream.aggregateAsyncWithin(aggregator, Schedule.spaced(timeout))
      )

    def terminateOnFiberFailure[E1 >: E](fib: Fiber[E1, Any]): ZStream[R, E1, O] =
      stream
        .map(Exit.succeed)
        .mergeHaltEither(ZStream.fromZIO(fib.join).as(Exit.fail(None)) *> ZStream.never)
        .flattenExitOption

    def terminateOnPromiseCompleted[E1 >: E](p: Promise[Nothing, _]): ZStream[R, E1, O] =
      stream.map(Exit.succeed).mergeHaltEither(ZStream.fromZIO(p.await).as(Exit.fail(None))).flattenExitOption

    def viaIf[R1 <: R, E1 >: E, O1 >: O](condition: Boolean)(
      f: ZStream[R, E, O] => ZStream[R1, E1, O1]
    ): ZStream[R1, E1, O1] =
      if (condition) f(stream) else stream

    def viaMatch[A, R1 <: R, E1 >: E, O1 >: O](value: A)(
      f: PartialFunction[A, ZStream[R, E, O] => ZStream[R1, E1, O1]]
    ): ZStream[R1, E1, O1] =
      if (f.isDefinedAt(value)) f(value)(stream) else stream
  }

  /**
   * Schedule for exponential backoff up to a maximum interval and an optional maximum number of retries
   *
   * @param min
   *   Minimum backoff time
   * @param max
   *   Maximum backoff time. When this value is reached, subsequent intervals will be equal to this value.
   * @param factor
   *   Exponential factor. 2 means doubling, 1 is constant, < 1 means decreasing
   * @param maxRecurs
   *   Maximum retries. When this number is exceeded, the schedule will end
   * @tparam A
   *   Schedule input
   */
  def exponentialBackoff[A](
    min: Duration,
    max: Duration,
    factor: Double = 2.0,
    maxRecurs: Option[Int] = None
  ): Schedule[Any, A, (Duration, Long)] =
    (Schedule.exponential(min, factor).whileOutput(_ <= max) andThen Schedule.fixed(max).as(max)) &&
      maxRecurs.map(Schedule.recurs).getOrElse(Schedule.forever)

  /**
   * Executes calls through a token bucket stream, ensuring a maximum rate of calls
   *
   * Allows for bursting
   *
   * @param units
   *   Maximum number of calls per duration
   * @param duration
   *   Duration for nr of tokens
   * @return
   *   The original function with rate limiting applied, as a managed resource
   */
  def throttledFunction[R, I, E, A](units: Int, duration: Duration)(
    f: I => ZIO[R, E, A]
  ): ZIO[Scope, Nothing, I => ZIO[R, E, A]] =
    for {
      requestsQueue <- Queue.bounded[(IO[E, A], Promise[E, A])](units / 2 * 2)
      _             <- ZStream
                         .fromQueueWithShutdown(requestsQueue, 1) // See https://github.com/zio/zio/issues/4190
                         .throttleShape(units.toLong, duration, units.toLong)(_ => 1)
                         .mapZIO { case (effect, promise) => promise.completeWith(effect) }
                         .runDrain
                         .forkScoped
    } yield (input: I) =>
      for {
        env     <- ZIO.environment[R]
        promise <- Promise.make[E, A]
        _       <- requestsQueue.offer((f(input).provideEnvironment(env), promise))
        result  <- promise.await
      } yield result

  def throttledFunctionN(units: Int, duration: Duration): ThrottledFunctionPartial =
    ThrottledFunctionPartial(units, duration)

  final case class ThrottledFunctionPartial(units: Int, duration: Duration) {
    def apply[R, I0, I1, E, A](f: (I0, I1) => ZIO[R, E, A]): ZIO[Scope, Nothing, (I0, I1) => ZIO[R, E, A]] =
      throttledFunction[R, (I0, I1), E, A](units, duration) { case (i0, i1) =>
        f(i0, i1)
      }.map(Function.untupled(_))

    def apply[R, I0, I1, I2, E, A](
      f: (I0, I1, I2) => ZIO[R, E, A]
    ): ZIO[Scope, Nothing, (I0, I1, I2) => ZIO[R, E, A]] =
      throttledFunction[R, (I0, I1, I2), E, A](units, duration) { case (i0, i1, i2) =>
        f(i0, i1, i2)
      }.map(Function.untupled(_))
  }

  /**
   * Creates a resource that executes `effect` with intervals of `period` or via manual invocation
   *
   * After manual invocation, the next effect execution will be after interval. Any triggers during effect execution are
   * ignored.
   *
   * @return
   *   ZIO that when executed, immediately starts execution of `effect`
   */
  def periodicAndTriggerableOperation[R, A](
    effect: ZIO[R, Nothing, A],
    period: Duration
  ): ZIO[Scope with R, Nothing, UIO[Unit]] =
    for {
      queue <- ZIO.acquireRelease(Queue.dropping[Unit](1))(_.shutdown)
      _     <- ((queue.take raceFirst ZIO.sleep(period)) *> effect *> queue.takeAll).forever.forkScoped // Fiber cannot fail
    } yield queue.offer(()).unit
}
