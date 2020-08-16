package nl.vroste.zio.kinesis.client

import software.amazon.awssdk.services.kinesis.model._
import zio.Schedule
import zio.clock.Clock
import zio.duration._

object AdminClient {

  private[client] val retryOnLimitExceeded = Schedule.recurWhile[Throwable] {
    case _: LimitExceededException => true; case _ => false
  }

  private[client] val defaultBackoffSchedule: Schedule[Clock, Any, Any] = Schedule.exponential(200.millis) && Schedule
    .recurs(5)
}
