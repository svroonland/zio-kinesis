package nl.vroste.zio.kinesis.client.producer

import nl.vroste.zio.kinesis.client.producer.ShardThrottler.DynamicThrottler
import zio.test.Assertion._
import zio.test._
import zio.{ ZIO, _ }

object ShardThrottlerTest extends ZIOSpecDefault {
  override def spec =
    suite("ShardThrottler")(
      test("does not throtle for zero throughput") {
        DynamicThrottler.make(1.second).flatMap { throttler =>
          for {
            _    <- TestClock.adjust(1.second)
            rate <- throttler.throughputFactor
          } yield assert(rate)(equalTo(1.0d))
        }
      },
      test("does not throtle for only successes") {
        DynamicThrottler.make(1.second).flatMap { throttler =>
          for {
            _    <- ZIO.collectAllDiscard(ZIO.replicate(100)(throttler.addSuccess))
            _    <- TestClock.adjust(1.second)
            rate <- throttler.throughputFactor
          } yield assert(rate)(equalTo(1.0d))
        }
      },
      test("throttles to the expected values") {
        val errorRate = 0.1
        DynamicThrottler.make(1.second, errorRate).flatMap { throttler =>
          for {
            _     <- ZIO.collectAllDiscard(ZIO.replicate(100)(throttler.addSuccess))
            _     <- ZIO.collectAllDiscard(ZIO.replicate(100)(throttler.addFailure))
            _     <- TestClock.adjust(1.second)
            rate1 <- throttler.throughputFactor
            _     <- ZIO.collectAllDiscard(ZIO.replicate(100)(throttler.addSuccess))
            _     <- ZIO.collectAllDiscard(ZIO.replicate(100)(throttler.addFailure))
            _     <- TestClock.adjust(1.second)
            rate2 <- throttler.throughputFactor
          } yield assert((rate1, rate2))(equalTo((0.5d + errorRate, 0.4)))
        }
      }
    )
}
