package nl.vroste.zio.kinesis.client.zionative.leasecoordinator
import zio.{ Exit, URIO, ZIO }

object ZioExtensions {
  implicit class OnSuccessSyntax[R, E, A](val zio: ZIO[R, E, A]) extends AnyVal {
    final def onSuccess(cleanup: A => URIO[R, Any]): ZIO[R, E, A] =
      zio.onExit {
        case Exit.Success(a) => cleanup(a)
        case _               => ZIO.unit
      }
  }
}
