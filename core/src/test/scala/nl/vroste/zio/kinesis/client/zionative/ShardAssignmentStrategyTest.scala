package nl.vroste.zio.kinesis.client.zionative

import nl.vroste.zio.kinesis.client.zionative.ShardAssignmentStrategy.{ leasesToHave, LeaseInfo }
import zio.test.{ Gen, ZIOSpecDefault, _ }

object ShardAssignmentStrategyTest extends ZIOSpecDefault {
  private def randomLeaseInfo =
    for {
      nrShards        <- Gen.int(1, 500)
      nrWorkers       <- Gen.int(1, 300) // More workers than shards
      thisWorker      <- Gen.int(1, nrWorkers).map(_.toString)
      workerIsExpired <- Gen.listOfN(nrWorkers)(Gen.boolean).map((1 to nrWorkers).zip(_)).map(_.toMap)
      workerForShard  <- Gen.listOfN(nrShards)(
                           Gen.oneOf(
                             // Decrease chance of unowned lease
                             Gen.int(1, nrWorkers).map(Some(_)),
                             Gen.int(1, nrWorkers).map(Some(_)),
                             Gen.int(1, nrWorkers).map(Some(_)),
                             Gen.int(1, nrWorkers).map(Some(_)),
                             Gen.none
                           )
                         )
    } yield thisWorker -> workerForShard.zipWithIndex.map { case (worker, shardNr) =>
      // Our own leases or those of other workers cannot be expired
      LeaseInfo(
        s"shard-${shardNr}",
        worker.map(_.toString),
        worker.exists(workerIsExpired(_)) && !worker.map(_.toString).contains(thisWorker) && worker.isDefined
      )
    }

  override def spec =
    suite("ShardAssignmentStrategy")(
      suite("balanced")(
        test(
          "wants to have its fair share of the list of leases it already owns + expired or unowned leases + leases from other workers in order of busiest"
        ) {
          check(randomLeaseInfo) { case (thisWorker, allLeases) =>
            val leaseMap = allLeases.map(l => l.key -> l).toMap
            for {
              desired <- leasesToHave(allLeases, thisWorker).map(_.map(leaseMap(_)))

              activeWorkers   = allLeases.filterNot(_.isExpired).flatMap(_.owner).toSet
              nrActiveWorkers = (activeWorkers + thisWorker).size

              minLeasesByWorker = Math.floor(allLeases.size * 1.0 / (nrActiveWorkers * 1.0)).toInt
              maxLeasesByWorker = minLeasesByWorker + (allLeases.size % nrActiveWorkers)
              nrOptionalLeases  = maxLeasesByWorker - minLeasesByWorker

              thisWorkerLeases  = desired.filter(_.owner.contains(thisWorker))
              freeDesiredLeases = desired.filter(l => l.owner.isEmpty || l.isExpired)

              stolenLeases      = (desired -- thisWorkerLeases -- freeDesiredLeases)
              stolenFromWorkers =
                stolenLeases.groupBy(_.owner.get).view.mapValues(_.size).toSeq.sortBy(_._2).reverse.map(_._1)

              otherActiveWorkerLeases =
                allLeases.filterNot(_.isExpired).filterNot(_.owner.contains(thisWorker)).filterNot(_.owner.isEmpty)
              busiestWorkers          =
                otherActiveWorkerLeases
                  .groupBy(_.owner.get)
                  .view
                  .mapValues(_.size)
                  .toSeq
                  .sortBy { case (workerId, nrLeases) => (nrLeases, workerId) }
                  .reverse
                  .map(_._1)

              freeLeases = allLeases.filter(l => l.owner.isEmpty || l.isExpired)

//              _  =
//                println(
//                  s"This worker: ${thisWorker}, all leases (${allLeases.size}): ${allLeases.mkString(",")}, desired (${desired.size}): ${desired.toSeq.sortBy(_.key).mkString(",")}, activeWorkers: ${nrActiveWorkers}, min = ${minLeasesByWorker}, max = ${maxLeasesByWorker}, free leases desired = ${freeDesiredLeases.size}, stolen from workers: ${stolenFromWorkers.mkString(",")}"
//                )
            } yield assertTrue(
              desired.size >= minLeasesByWorker &&
                desired.size <= maxLeasesByWorker &&
                // It should desire its own leases first
                thisWorkerLeases.size >= Math.min(allLeases.count(_.owner.contains(thisWorker)), minLeasesByWorker) &&
                // If taking more than our own leases, we should prioritize expired and unowned
                (desired.size <= thisWorkerLeases.size || (freeDesiredLeases
                  .take(nrOptionalLeases)
                  .size >= Math.min(nrOptionalLeases, freeLeases.size))) &&
                // If stealing leases, it should have prioritized the busiest workers
                stolenFromWorkers == busiestWorkers.take(stolenFromWorkers.size)
            )
          }

        }
      )
    ) @@ TestAspect.samples(1000)
}
