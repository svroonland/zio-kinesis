package nl.vroste.zio.kinesis

import zio.{ Fiber, Promise, UIO, ZIO, ZManaged }

package object client {

  /**
   * Run an effect that can be gracefully shutdown on interruption
   *
   * The effect is run in a fiber
   *
   * @param f Create the effect to run, given a UIO that will complete to signal graceful interruption
   * @return The fiber running the effect created by f as a ZManaged. The effect will be gracefully
   *   shutdown when the ZManaged is released
   */
  def withGracefulShutdownOnInterrupt[R, E, A](
    f: UIO[Unit] => ZIO[R, E, A]
  ): ZManaged[R, Nothing, Fiber.Runtime[E, A]] =
    for {
      interrupt <- Promise.make[Throwable, Unit].toManaged_
      result    <- f(interrupt.await.ignore).fork.toManaged(_.join.ignore)
      _         <- ZManaged.finalizer(interrupt.succeed(()))
    } yield result
}
