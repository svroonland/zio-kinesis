package nl.vroste.zio.kinesis.client.zionative

import nl.vroste.zio.kinesis.client.zionative.LeaseRepository.Lease
import zio.{ ZIO, _ }

import java.time.Instant

/**
 * Decides which shards this worker would like to have
 */
trait ShardAssignmentStrategy {

  /**
   * Given the current lease assignment, pick which shards this worker desires
   *
   * Leases for picked shards which don't yet exist will be created, leases owned by other workers will be stolen. Note
   * that all of this may fail when other workers are active in the pool, so it is not guaranteed that this worker
   * actually gets its desired shards. However, on the next lease assignment interval, the strategy can indicate its
   * desired shards again based.
   *
   * Leases for shards that are not desired (anymore) will be released.
   *
   * @param leases
   *   Current lease assignments + time of last update
   * @param shards
   *   IDs of all shards of the stream
   * @param workerId
   *   ID of current worker, reflected in the 'owner' property of leases
   * @return
   */
  def desiredShards(
    leases: Set[(Lease, Instant)],
    shards: Set[String],
    workerId: String
  ): ZIO[Any, Nothing, Set[String]]
}

object ShardAssignmentStrategy {

  /**
   * Manual shard assignment for a fixed set of shards
   *
   * Note that users must make sure that other workers in the pool also use a manual shard assignment strategy,
   * otherwise stealing back of leases by other workers may occur.
   *
   * You also need to make sure that all shards are covered by the shard assignment of all workers. After resharding,
   * the list of shards must be reconfigured.
   *
   * @param shardAssignment
   *   IDs of shards to assign to this worker
   */
  def manual(shardAssignment: Set[String]) =
    new ShardAssignmentStrategy {
      override def desiredShards(
        leases: Set[(Lease, Instant)],
        shards: Set[String],
        workerId: String
      ): ZIO[Any, Nothing, Set[String]] =
        ZIO.succeed(shardAssignment intersect shards)
    }

  // TODO possible future strategy: manual + fallback
  // When no other worker has active leases for other shards, take them over

  private[zionative] case class LeaseInfo(key: String, owner: Option[String], isExpired: Boolean)

  /**
   * Balances shard assignments between a set of workers by taking / stealing leases so that each has its fair share
   *
   * Shards which do not yet have a lease will be claimed by this worker. To get a fair share of leases, the desired
   * shards may contain leases to steal from other workers. The most shards will be selected from the busiest worker. A
   * random selection is made within each worker's shards so that the chance of contention with other stealing workers
   * is reduced.
   *
   * @param expirationTime
   *   Time after which a lease is considered expired if not updated in the meantime This should be set to several times
   *   the lease renewal interval so that leases are not considered expired until several renew intervals have been
   *   missed, in case of transient connection issues.
   */
  def balanced(expirationTime: Duration = 10.seconds) =
    new ShardAssignmentStrategy {
      override def desiredShards(
        leases: Set[(Lease, Instant)],
        shards: Set[String],
        workerId: String
      ): ZIO[Any, Nothing, Set[String]] =
        for {
          now               <- zio.Clock.currentDateTime.map(_.toInstant())
          shardsWithoutLease = shards.filterNot(shard => leases.map(_._1.key).toList.contains(shard))
          allLeases          = leases.map { case (lease, lastUpdated) =>
                                 LeaseInfo(
                                   lease.key,
                                   lease.owner,
                                   isExpired = lastUpdated.isBefore(now.minusMillis(expirationTime.toMillis))
                                 )
                               } ++ shardsWithoutLease.map(LeaseInfo(_, None, isExpired = false))

          toHave <- leasesToHave(allLeases.toSeq, workerId)
        } yield toHave

    }

  /**
   * Compute which leases to take, either without owner or from other workers
   *
   * We take expired and unowned leases first. We only steal if necessary and do it from the busiest worker first. We
   * will not steal more than the other worker's target (nr leases / nr workers).
   *
   * @param allLeases
   *   Latest known state of the all leases
   * @param workerId
   *   ID of this worker
   * @return
   *   List of leases that should be taken by this worker
   */
  private[zionative] def leasesToHave(
    allLeases: Seq[LeaseInfo],
    workerId: String
  ): ZIO[Any, Nothing, Set[String]] = {
    // Determine min and optional target number of leases to have
    val expiredLeases = allLeases.filter(_.isExpired)
    val activeWorkers =
      (allLeases.toSet -- expiredLeases).flatMap(_.owner) + workerId

    val minTarget = Math.floor(allLeases.size * 1.0 / (activeWorkers.size * 1.0)).toInt

    // If the nr of workers does not evenly divide the shards, there's some leases that at least one worker should take
    // These we will not steal, only take
    val optional = allLeases.size % activeWorkers.size

    def logPlan(ourLeases: Seq[LeaseInfo]) = {
      val minNrLeasesToTake = Math.max(0, minTarget - ourLeases.size)
      val allWorkers        = allLeases.flatMap(_.owner).toSet + workerId
      val zombieWorkers     = allWorkers -- activeWorkers
      ZIO.logInfo(
        s"We have ${ourLeases.size}, we would like to have at least ${minTarget}/${allLeases.size} leases (${activeWorkers.size} active workers, " +
          s"${zombieWorkers.size} zombie workers), we need ${minNrLeasesToTake} more with an optional ${optional}"
      ) *> ZIO.logInfo(s"Found expired leases: ${expiredLeases.map(_.key).mkString(",")}").when(expiredLeases.nonEmpty)
    }

    for {
      randomAllLeases <- ZIO.randomWith(_.shuffle(allLeases))
      ourLeases        = randomAllLeases.filter(_.owner.contains(workerId))
      unownedLeases    = randomAllLeases.filter(_.owner.isEmpty)
      expiredLeases    = randomAllLeases.filter(_.isExpired)

      _          <- logPlan(ourLeases)
      ownedLeases = randomAllLeases.filter { lease =>
                      !lease.isExpired && lease.owner.isDefined && !lease.owner.contains(workerId)
                    }
      // Surpluses leases, with the busiest worker first
      surpluses   = ownedLeases
                      .groupBy(_.owner)
                      .values
                      .map(_.drop(minTarget))
                      .filter(_.nonEmpty)
                      .toSeq
                      .sortBy(leases => (leases.size, leases.head.owner.get))
                      .reverse
                      .flatten

      randomFreeLeases <- ZIO.randomWith(_.shuffle(unownedLeases ++ expiredLeases))
      sortedLeases      = ourLeases ++ randomFreeLeases ++ surpluses

      (targetLeases, optionalLeases) = sortedLeases.splitAt(minTarget)
      leasesToTake                   = (targetLeases ++ optionalLeases.take(optional).takeWhile(l => l.owner.isEmpty || l.isExpired))

    } yield leasesToTake.map(_.key).toSet
  }
}
