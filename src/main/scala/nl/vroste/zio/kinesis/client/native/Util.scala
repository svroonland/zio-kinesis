package nl.vroste.zio.kinesis.client.native
import zio.{ IO, Promise, Queue, Schedule, ZIO, ZManaged }
import zio.clock.Clock
import zio.duration.Duration
import zio.stream.ZStream

object Util {
  // TODO add jitter
  def exponentialBackoff(
    min: Duration,
    max: Duration,
    factor: Double = 2.0,
    maxRecurs: Option[Int] = None
  ): Schedule[Clock, Throwable, Any] =
    (Schedule.exponential(min).whileOutput(_ <= max) andThen Schedule.fixed(max)) &&
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
  def throttledFunction[R, I, E, A](
    units: Long,
    duration: Duration,
    f: I => ZIO[R, E, A]
  ): ZManaged[Clock, Nothing, I => ZIO[R, E, A]] =
    for {
      requestsQueue <- Queue.unbounded[(IO[E, A], Promise[E, A])].toManaged_
      stream        <- ZStream
                  .fromQueueWithShutdown(requestsQueue)
                  .throttleShape(units, duration, units)(_ => 1)
                  .mapM { case (effect, promise) => promise.complete(effect) }
                  .runDrain
                  .forkManaged
    } yield input =>
      for {
        env     <- ZIO.environment[R]
        promise <- Promise.make[E, A]
        _       <- requestsQueue.offer((f(input).provide(env), promise))
        result  <- promise.await
      } yield result
}
