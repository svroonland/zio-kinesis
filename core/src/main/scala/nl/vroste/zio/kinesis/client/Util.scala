package nl.vroste.zio.kinesis.client

import zio._
import zio.clock.Clock
import zio.duration._
import zio.stream.ZStream

object Util {
  implicit class ZStreamExtensions[-R, +E, +O](val stream: ZStream[R, E, O]) extends AnyVal {
    // ZStream's groupBy using distributedWithDynamic is not performant enough, maybe because
    // it breaks chunks
    final def groupByKey2[K](
      getKey: O => K,
      substreamChunkBuffer: Int = 32 // Number of chunks to buffer per substream
    ): ZStream[R, E, (K, ZStream[Any, E, O])] =
      ZStream.unwrapManaged {
        type GroupQueueValues = Exit[Option[E], Chunk[O]]

        for {
          substreamsQueue     <- Queue
                                   .unbounded[Exit[Option[E], (K, Queue[GroupQueueValues])]]
                                   .toManaged(_.shutdown)
          substreamsQueuesMap <- Ref.make(Map.empty[K, Queue[GroupQueueValues]]).toManaged_
          _                   <- {
            val addToSubStream: (K, Chunk[O]) => ZIO[Any, Nothing, Unit] = (key: K, values: Chunk[O]) =>
              for {
                substreams <- substreamsQueuesMap.get
                _          <- if (substreams.contains(key))
                                substreams(key).offer(Exit.succeed(values))
                              else
                                Queue
                                  .bounded[GroupQueueValues](substreamChunkBuffer)
                                  .tap(_.offer(Exit.succeed(values)))
                                  .tap(q => substreamsQueue.offer(Exit.succeed((key, q))))
                                  .tap(q => substreamsQueuesMap.update(_ + (key -> q)))
                                  .unit
              } yield ()
            (stream.foreachChunk { chunk =>
              ZIO.foreach_(chunk.groupBy(getKey))(addToSubStream.tupled)
            } *> substreamsQueue.offer(Exit.fail(None))).catchSome { case e =>
              substreamsQueue.offer(Exit.fail(Some(e)))
            }.forkManaged
          }
        } yield ZStream.fromQueueWithShutdown(substreamsQueue).flattenExitOption.map { case (key, substreamQueue) =>
          val substream = ZStream
            .fromQueueWithShutdown(substreamQueue)
            .flattenExitOption
            .flattenChunks
          (key, substream)
        }
      }
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
  ): Schedule[Clock, A, (Duration, Long)] =
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
      throttledFunction[R, (I0, I1), E, A](units, duration) { case (i0, i1) =>
        f(i0, i1)
      }.map(Function.untupled(_))

    def apply[R, I0, I1, I2, E, A](
      f: (I0, I1, I2) => ZIO[R, E, A]
    ): ZManaged[Clock, Nothing, (I0, I1, I2) => ZIO[R, E, A]] =
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
  ): ZManaged[R with Clock, Nothing, UIO[Unit]] =
    for {
      queue <- Queue.dropping[Unit](1).toManaged(_.shutdown)
      _     <- ((queue.take raceFirst ZIO.sleep(period)) *> effect *> queue.takeAll).forever.forkManaged
    } yield queue.offer(()).unit
}
