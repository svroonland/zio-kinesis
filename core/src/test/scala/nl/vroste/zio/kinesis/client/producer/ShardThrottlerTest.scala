package nl.vroste.zio.kinesis.client.producer
import nl.vroste.zio.kinesis.client.producer.ShardThrottler.DynamicThrottler
import zio.ZIO
import zio.test._
import zio.duration._
import zio.test.Assertion._
import zio.test.environment.TestClock

object ShardThrottlerTest extends DefaultRunnableSpec {
  override def spec =
    suite("ShardThrottler")(
      testM("does not throtle for zero throughput") {
        DynamicThrottler.make(1.second).use { throttler =>
          for {
            _    <- TestClock.adjust(1.second)
            rate <- throttler.throughputFactor
          } yield assert(rate)(equalTo(1.0d))
        }
      },
      testM("does not throtle for only successes") {
        DynamicThrottler.make(1.second).use { throttler =>
          for {
            _    <- ZIO.collectAll_(ZIO.replicate(100)(throttler.addSuccess))
            _    <- TestClock.adjust(1.second)
            rate <- throttler.throughputFactor
          } yield assert(rate)(equalTo(1.0d))
        }
      },
      testM("throttles to the expected values") {
        val errorRate = 0.1
        DynamicThrottler.make(1.second, errorRate).use { throttler =>
          for {
            _     <- ZIO.collectAll_(ZIO.replicate(100)(throttler.addSuccess))
            _     <- ZIO.collectAll_(ZIO.replicate(100)(throttler.addFailure))
            _     <- TestClock.adjust(1.second)
            rate1 <- throttler.throughputFactor
            _     <- ZIO.collectAll_(ZIO.replicate(100)(throttler.addSuccess))
            _     <- ZIO.collectAll_(ZIO.replicate(100)(throttler.addFailure))
            _     <- TestClock.adjust(1.second)
            rate2 <- throttler.throughputFactor
          } yield assert((rate1, rate2))(equalTo((0.5d + errorRate, 0.4)))
        }
      },
      testM("throttles the correct shard") {
        val errorRate = 0.1
        ShardThrottler.make(1.second, errorRate).use { throttler =>
          for {
            _      <- ZIO.collectAll_(ZIO.replicate(100)(throttler.addSuccess("1")))
            _      <- ZIO.collectAll_(ZIO.replicate(100)(throttler.addFailure("1")))
            _      <- TestClock.adjust(1.second)
            rates1 <- throttler.throughputFactor("1") <*> throttler.throughputFactor("2")
            _      <- ZIO.collectAll_(ZIO.replicate(100)(throttler.addSuccess("2")))
            _      <- ZIO.collectAll_(ZIO.replicate(100)(throttler.addFailure("2")))
            _      <- TestClock.adjust(1.second)
            rates2 <- throttler.throughputFactor("1") <*> throttler.throughputFactor("2")
          } yield assert((rates1, rates2))(equalTo(((0.6d, 1.0d), (0.7d, 0.6d))))
        }
      }
    )
}
