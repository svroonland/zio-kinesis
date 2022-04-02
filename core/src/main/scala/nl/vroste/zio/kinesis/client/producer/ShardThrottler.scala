package nl.vroste.zio.kinesis.client.producer
import nl.vroste.zio.kinesis.client.producer.ShardThrottler.DynamicThrottler
import zio.{ UIO, _ }

import zio.Clock

private[client] trait ShardThrottler {
  // TODO deprecate
  def throughputFactor(shard: String): UIO[Double]
  def addSuccess(shard: String): UIO[Unit]
  def addFailure(shard: String): UIO[Unit]
  def getForShard(shardId: String): UIO[DynamicThrottler]
}

private[client] object ShardThrottler {
  def make(
    updatePeriod: Duration = 5.seconds,
    allowedError: Double = 0.02
  ): ZIO[Scope with Clock, Nothing, ShardThrottler] =
    for {
      scope  <- ZIO.scope
      clock  <- ZIO.environment[Clock]
      shards <- Ref.make(Map.empty[String, DynamicThrottler])
    } yield new ShardThrottler {
      override def throughputFactor(shard: String): UIO[Double] = withShard(shard, _.throughputFactor)
      override def addSuccess(shard: String): UIO[Unit]         = withShard(shard, _.addSuccess)
      override def addFailure(shard: String): UIO[Unit]         = withShard(shard, _.addFailure)

      def withShard[E, A](shard: String, f: DynamicThrottler => IO[E, A]): IO[E, A] =
        getForShard(shard)
          .flatMap(f)

      override def getForShard(shardId: String): UIO[DynamicThrottler] =
        shards.get.flatMap { throttlers =>
          if (throttlers.contains(shardId)) ZIO.succeed(throttlers(shardId))
          else
            for {
              throttler <- scope.extend(DynamicThrottler.make(updatePeriod, allowedError))
              _         <- shards.update(_ + (shardId -> throttler))
            } yield throttler
        }.provideEnvironment(clock)
    }

  trait DynamicThrottler {
    def throughputFactor: UIO[Double]
    def addSuccess: UIO[Unit]
    def addFailure: UIO[Unit]
  }

  object DynamicThrottler {

    // Do we measure in time or in nr of (Kinesis) records?
    def make(
      updatePeriod: Duration = 5.seconds,
      allowedError: Double = 0.1
    ): ZIO[Scope with Clock, Nothing, DynamicThrottler] =
      for {
        counter          <- Ref.make[(Long, Long)]((0, 0))
        successRate      <- Ref.make(1.0d)
        updateSuccessRate = for {
                              counts                 <- counter.getAndSet((0, 0))
                              currentSuccessRate      = counts match {
                                                          case (successes, failures) =>
                                                            if ((successes + failures) > 0)
                                                              successes * 1.0d / (successes + failures)
                                                            else 1.0
                                                        }
                              successRateUpdateFactor = currentSuccessRate
                              _                      <- successRate.updateAndGet(r => (r * successRateUpdateFactor + allowedError) min 1.0)
                            } yield ()
        _                <- updateSuccessRate
                              .repeat(Schedule.spaced(updatePeriod))
                              .delay(updatePeriod)
                              .forkScoped
      } yield new DynamicThrottler {
        override final def throughputFactor: UIO[Double] = successRate.get
        override final def addSuccess: UIO[Unit]         = update(addSuccess = 1)
        override final def addFailure: UIO[Unit]         = update(addFailure = 1)

        @inline private final def update(addSuccess: Int = 0, addFailure: Int = 0) =
          counter.update { case (successes, failures) => (successes + addSuccess, failures + addFailure) }
      }
  }
}
