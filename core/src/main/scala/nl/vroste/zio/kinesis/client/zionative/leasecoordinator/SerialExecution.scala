package nl.vroste.zio.kinesis.client.zionative.leasecoordinator
import zio.stm.{ TMap, TSemaphore, ZSTM }
import zio.{ Scope, ZIO }

/**
 * Ensures that effects are run serially per key
 *
 * @tparam K
 *   Type of key
 */
trait SerialExecution[K] {
  def apply[R, E, A](key: K)(f: ZIO[R, E, A]): ZIO[R, E, A]
}

object SerialExecution {
  def keyed[K]: ZIO[Scope, Nothing, SerialExecution[K]] =
    for {
      locks <- TMap.empty[K, TSemaphore].commit
    } yield new SerialExecution[K] {
      override def apply[R, E, A](key: K)(f: ZIO[R, E, A]): ZIO[R, E, A] =
        for {
          lock   <- createOrGetLockForKey(key).commit
          result <- lock.withPermit(f)
        } yield result

      private def createOrGetLockForKey(key: K): ZSTM[Any, Nothing, TSemaphore] =
        for {
          lockOpt <- locks.get(key)
          newLock <- lockOpt match {
                       case Some(lock) => ZSTM.succeed(lock)
                       case None       => TSemaphore.make(1).tap(locks.put(key, _))
                     }
        } yield newLock
    }

}
