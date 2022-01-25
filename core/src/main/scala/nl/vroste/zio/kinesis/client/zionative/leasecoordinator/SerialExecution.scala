package nl.vroste.zio.kinesis.client.zionative.leasecoordinator
import zio.{ Ref, Semaphore, UManaged, ZIO }

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
  def keyed[K]: UManaged[SerialExecution[K]] =
    for {
      locks <- Ref.make(Map.empty[K, Semaphore]).toManaged_
    } yield new SerialExecution[K] {
      override def apply[R, E, A](key: K)(f: ZIO[R, E, A]): ZIO[R, E, A] =
        for {
          lockOpt <- locks.get.map(_.get(key))
          lock    <- lockOpt match {
                       case Some(lock) => ZIO.succeed(lock)
                       case None       =>
                         for {
                           // There's a race condition between getting a None from the locks map and putting the new Semaphore there, hence we
                           // atomically check again. The unused created Semaphore will get GC'd
                           newLock        <- Semaphore.make(1)
                           newLockUpdated <- locks.modify { locks =>
                                               locks.get(key) match {
                                                 case Some(lock) => (lock, locks)
                                                 case None       => (newLock, locks + (key -> newLock))
                                               }
                                             }
                         } yield newLockUpdated
                     }
          result  <- lock.withPermit(f)
        } yield result
    }
}
