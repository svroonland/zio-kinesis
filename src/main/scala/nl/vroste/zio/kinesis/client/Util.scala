package nl.vroste.zio.kinesis.client

import zio.stream.ZStream
import zio.Schedule
import zio.Ref
import zio.ZIO
import java.util.concurrent.CompletableFuture
import zio.clock.Clock
import zio.duration.Duration
import zio.Queue
import zio.Promise
import zio.ZManaged
import zio.Task
import zio.IO
import java.util.concurrent.CompletionException

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
    def retry[R1 <: R](schedule: Schedule[R1, E, _]): ZStream[R1, E, O] =
      ZStream.unwrap {
        for {
          s0    <- schedule.initial
          state <- Ref.make[schedule.State](s0)
        } yield {
          def go: ZStream[R1, E, O] =
            stream
              .catchAll(e =>
                ZStream.unwrap {
                  (for {
                    s        <- state.get
                    newState <- schedule.update(e, s)
                  } yield newState).fold(
                    _ => ZStream.fail(e), // Failure of the schedule indicates it doesn't accept the input
                    newState =>
                      ZStream.fromEffect(state.set(newState)) *> go.mapChunksM { chunk =>
                        // Reset the schedule to its initial state when a chunk is successfully pulled
                        state.set(s0).as(chunk)
                      }
                  )
                }
              )

          go
        }
      }
  }

  def asZIO[T](f: => CompletableFuture[T]): Task[T] =
    ZIO
      .effect(f)
      .flatMap(ZIO.fromCompletionStage(_))
      .catchSome {
        case e: CompletionException =>
          ZIO.fail(Option(e.getCause()).getOrElse(e))
      }

  def paginatedRequest[R, E, A, Token](fetch: Option[Token] => ZIO[R, E, (A, Option[Token])])(
    throttling: Schedule[Clock, Any, Int] = Schedule.forever
  ): ZStream[Clock with R, E, A] =
    ZStream.fromEffect(fetch(None)).flatMap {
      case (results, nextTokenOpt) =>
        ZStream.succeed(results) ++ (nextTokenOpt match {
          case None            => ZStream.empty
          case Some(nextToken) =>
            ZStream.paginateM[R, E, A, Token](nextToken)(token => fetch(Some(token))).scheduleElements(throttling)
        })
    }

  def exponentialBackoff[A](
    min: Duration,
    max: Duration,
    factor: Double = 2.0,
    maxRecurs: Option[Int] = None
  ): Schedule[Clock, A, (Duration, Int)] =
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
  def throttledFunction[R, I, E, A](units: Long, duration: Duration)(
    f: I => ZIO[R, E, A]
  ): ZManaged[Clock, Nothing, I => ZIO[R, E, A]] =
    for {
      requestsQueue <- Queue.unbounded[(IO[E, A], Promise[E, A])].toManaged_
      _             <- ZStream
             .fromQueueWithShutdown(requestsQueue)
             .throttleShape(units, duration, units)(_ => 1)
             .mapM { case (effect, promise) => promise.complete(effect) }
             .runDrain
             .forkManaged
    } yield (input: I) =>
      for {
        env     <- ZIO.environment[R]
        promise <- Promise.make[E, A]
        _       <- requestsQueue.offer((f(input).provide(env), promise))
        result  <- promise.await
      } yield result

  def throttledFunctionN[R, I0, I1, E, A](units: Long, duration: Duration)(
    f: (I0, I1) => ZIO[R, E, A]
  ): ZManaged[Clock, Nothing, ((I0, I1)) => ZIO[R, E, A]] =
    throttledFunction[R, (I0, I1), E, A](units, duration) {
      case (i0, i1) => f(i0, i1)
    }

}
