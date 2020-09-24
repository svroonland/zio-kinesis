package nl.vroste.zio.kinesis.client.zionative.leasecoordinator
import zio.stream.ZStream
import zio.{ Promise, Queue, UIO, UManaged, ZIO }

/**
 * Ensures that effects are run serially per key
 *
 * @tparam K Type of key
 */
trait SerialExecution[K] {
  def apply[R, E, A](key: K)(f: ZIO[R, E, A]): ZIO[R, E, A]
}

object SerialExecution {
  def keyed[K](queueSize: Int = 128): UManaged[SerialExecution[K]] =
    for {
      queue <- Queue
                 .bounded[(K, UIO[Unit])](queueSize)
                 .toManaged_
      _     <- ZStream
             .fromQueue(queue)
             .groupByKey(_._1) {
               case (key @ _, actions) =>
                 actions.mapM { case (_, action) => action }
             }
             .runDrain
             .forkManaged
    } yield new SerialExecution[K] {
      override def apply[R, E, A](key: K)(f: ZIO[R, E, A]): ZIO[R, E, A] =
        for {
          p      <- Promise.make[E, A]
          env    <- ZIO.environment[R]
          action  = f.provide(env).foldM(p.fail, p.succeed).unit
          _      <- queue.offer((key, action))
          result <- p.await
        } yield result
    }
}
