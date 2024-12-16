package nl.vroste.zio.kinesis.client.zionative

import java.time.Instant
import nl.vroste.zio.kinesis.client.zionative.LeaseRepository.Lease
import zio.ZIO

import zio._
import zio.Random.shuffle

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
          now          <- zio.Clock.currentDateTime.map(_.toInstant())
          expiredLeases = leases.collect {
                            case (lease, lastUpdated)
                                if lastUpdated.isBefore(now.minusMillis(expirationTime.toMillis)) &&
                                  !lease.checkpoint.contains(Left(SpecialCheckpoint.ShardEnd)) =>
                              lease
                          }
          _            <-
            ZIO.logInfo(s"Found expired leases: ${expiredLeases.map(_.key).mkString(",")}").when(expiredLeases.nonEmpty)

          shardsWithoutLease = shards.filterNot(shard => leases.map(_._1.key).toList.contains(shard))

          toTake             <- leasesToTake(leases.map(_._1).toList, workerId, expiredLeases.toList)
          shardsWeAlreadyHave = leases.collect { case (lease, _) if lease.owner.contains(workerId) => lease.key }
        } yield shardsWeAlreadyHave ++ shardsWithoutLease ++ toTake

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
   * @param expiredLeases
   *   Leases that have expired
   * @return
   *   List of leases that should be taken by this worker
   */
  def leasesToTake(
    allLeases: List[Lease],
    workerId: String,
    expiredLeases: List[Lease] = List.empty
  ): ZIO[Any, Nothing, Set[String]] = {
    val allWorkers    = allLeases.map(_.owner).collect { case Some(owner) => owner }.toSet + workerId
    val activeWorkers =
      (allLeases.toSet -- expiredLeases).map(_.owner).collect { case Some(owner) => owner } + workerId
    val zombieWorkers = allWorkers -- activeWorkers

    val minTarget = Math.floor(allLeases.size * 1.0 / (activeWorkers.size * 1.0)).toInt

    // If the nr of workers does not evenly divide the shards, there's some leases that at least one worker should take
    // These we will not steal, only take
    val optional = allLeases.size % activeWorkers.size

    val target = minTarget

    val ourLeases     = allLeases.filter(_.owner.contains(workerId))
    val unownedLeases = allLeases.filter(_.owner.isEmpty)

    val minNrLeasesToTake = Math.max(0, target - ourLeases.size)
    val maxNrLeasesToTake = Math.max(0, target + optional - ourLeases.size)

    // We may already own some leases
    ZIO.logInfo(
      s"We have ${ourLeases.size}, we would like to have at least ${target}/${allLeases.size} leases (${activeWorkers.size} active workers, " +
        s"${zombieWorkers.size} zombie workers), we need ${minNrLeasesToTake} more with an optional ${optional}"
    ) *> (if (minNrLeasesToTake > 0 || unownedLeases.nonEmpty || expiredLeases.nonEmpty)
            for {
              leasesWithoutOwner         <- shuffle(unownedLeases)
              leasesExpired              <- shuffle(expiredLeases)
              leasesWithoutOwnerOrExpired = (leasesWithoutOwner ++ leasesExpired).take(maxNrLeasesToTake)

              // We can only steal from our target budget, not the optional ones
              remaining = Math.min(maxNrLeasesToTake, Math.max(0, minNrLeasesToTake - leasesWithoutOwnerOrExpired.size))
              toSteal  <- leasesToSteal(allLeases, workerId, target, nrLeasesToSteal = remaining)
            } yield (leasesWithoutOwnerOrExpired ++ toSteal).map(_.key).toSet
          else ZIO.succeed(Set.empty[String]))
  }

  /**
   * Computes leases to steal from other workers
   *
   * @param allLeases
   *   Latest known state of the all leases
   * @param workerId
   *   ID of this worker
   * @param target
   *   Target number of leases for this worker
   * @param nrLeasesToSteal
   *   How many leases to steal to get to the target
   * @return
   *   List of leases that should be stolen
   */
  def leasesToSteal(
    allLeases: List[Lease],
    workerId: String,
    target: Int,
    nrLeasesToSteal: Int
  ): ZIO[Any, Nothing, List[Lease]] = {
    val leasesByWorker =
      allLeases.groupBy(_.owner).collect { case (Some(owner), leases) => owner -> leases }
    val allWorkers     = allLeases.map(_.owner).collect { case Some(owner) => owner }.toSet ++ Set(workerId)
    // println(s"Planning to steal ${nrLeasesToSteal} leases")

    // From busiest to least busy
    val nrLeasesByWorker  = allWorkers
      .map(worker => worker -> allLeases.count(_.owner.contains(worker)))
      .toMap
      .view
      .filterKeys(_ != workerId)
      .toList
      .sortBy { case (worker, nrLeases) =>
        (nrLeases * -1, worker)
      } // Sort desc by nr of leases and then asc by worker ID for deterministic sort order
    // println(s"Nr Leases by worker: ${nrLeasesByWorker}")
    val spilloverByWorker = nrLeasesByWorker.map { case (worker, nrLeases) =>
      worker -> Math.max(0, nrLeases - target)
    }
    // println(s"Spillover: ${spilloverByWorker}")

    // Determine how many leases to take from each worker
    val nrLeasesToStealByWorker = spilloverByWorker
      .foldLeft((nrLeasesToSteal, Map.empty[String, Int])) {
        case ((leasesToStealLeft, nrByWorker), (worker, spillover)) =>
          val toTake = Math.min(spillover, leasesToStealLeft)

          (leasesToStealLeft - toTake, nrByWorker + (worker -> toTake))
      }
      ._2
      .filter(_._2 > 0)
    // println(s"Going to steal from workers ${nrLeasesToStealByWorker}")

    // From each worker that we want to take some leases, randomize the leases to reduce contention
    for {
      leasesToStealByWorker <- ZIO.foreach(nrLeasesToStealByWorker.toList) { case (worker, nrLeasesToTake) =>
                                 shuffle(leasesByWorker.getOrElse(worker, List.empty))
                                   .map(_.take(nrLeasesToTake))
                               }
      leasesToSteal          = leasesToStealByWorker.flatten
    } yield leasesToSteal
  }

}
