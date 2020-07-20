package nl.vroste.zio.kinesis.client.zionative.leasecoordinator
import zio.{ Cause, Exit, URIO, ZIO }

object ZioExtensions {
  implicit class OnSuccessSyntax[R, E, A](val zio: ZIO[R, E, A]) extends AnyVal {
    final def onSuccess(cleanup: A => URIO[R, Any]): ZIO[R, E, A] =
      zio.onExit {
        case Exit.Success(a) => cleanup(a)
        case _               => ZIO.unit
      }
  }

  //
  /**
   * Like foreachParN_ but does not interrupt on failure of one of the effects
   *
   * When failed, the cause of the failure contains all causes
   */
  def foreachParNUninterrupted_[R, E, A, B](
    n: Int
  )(as: Iterable[A])(fn: A => ZIO[R, E, B]): ZIO[R, E, Unit] =
    ZIO
      .foreachParN(n)(as)(fn(_).cause)
      .map(_.reduceOption(_ && _).getOrElse(Cause.empty))
      .uncause
}
