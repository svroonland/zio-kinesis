package nl.vroste.zio.kinesis.client

import java.util.concurrent.{ CompletableFuture, CompletionException }

import software.amazon.awssdk.core.exception.SdkException
import zio.{ Schedule, Task, ZIO }
import zio.clock.Clock
import zio.duration.Duration
import zio.stream.ZStream

object Util {
  def asZIO[T](f: => CompletableFuture[T]): Task[T] =
    ZIO
      .effect(f)
      .flatMap(ZIO.fromCompletionStage(_))
      .catchSome {
        case e: CompletionException =>
          ZIO.fail(Option(e.getCause).getOrElse(e))
        case e: SdkException        =>
          ZIO.fail(Option(e.getCause).getOrElse(e))
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
}
