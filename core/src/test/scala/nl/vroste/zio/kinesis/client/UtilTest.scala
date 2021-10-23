package nl.vroste.zio.kinesis.client


import zio.test.Assertion._
import zio.test._
import zio._

object UtilTest extends DefaultRunnableSpec {

  override def spec =
    suite("Util")(
      suite("exponentialBackoff")(
        test("provides exponential backoff durations up to the max") {
          val schedule = Util.exponentialBackoff[Int](100.millis, 1.second)

          for {
            now     <- zio.Clock.currentDateTime
            outputs <- schedule.run(now, (1 to 100))
          } yield assert(outputs.toList.map(_._1))(forall(Assertion.isLessThanEqualTo(1.second)))
        },
        test("stops after the max nr of recurs") {
          val maxRetries = 10
          val schedule   = Util.exponentialBackoff[Int](100.millis, 1.second, maxRecurs = Some(maxRetries))

          for {
            now     <- zio.Clock.currentDateTime
            outputs <- schedule.run(now, (1 to 100))
          } yield assert(outputs.toList)(hasSize(equalTo(maxRetries + 1)))
        }
      )
    )
}
