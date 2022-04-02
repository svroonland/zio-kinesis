package nl.vroste.zio.kinesis.client.zionative.leasecoordinator
import zio.stm.TMap
import zio.{ Scope, Semaphore, ZIO }

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
      locks <- TMap.empty[K, Semaphore].commit
    } yield new SerialExecution[K] {
      override def apply[R, E, A](key: K)(f: ZIO[R, E, A]): ZIO[R, E, A] =
        for {
          lockOpt <- locks.get(key).commit
          lock    <- lockOpt.fold(createLockForKey(key))(ZIO.succeed(_))
          result  <- lock.withPermit(f)
        } yield result

      private def createLockForKey(key: K): ZIO[Any, Nothing, Semaphore] =
        for {
          // There's a race condition between getting a None from the locks map and putting the new Semaphore there, hence we
          // atomically check again. The unused created Semaphore will get GC'd
          newLock        <- Semaphore.make(1)
          newLockUpdated <- (for {
                              _              <- locks.putIfAbsent(key, newLock)
                              newLockUpdated <- locks.getOrElse(key, newLock)
                            } yield newLockUpdated).commit
        } yield newLockUpdated
    }

}
