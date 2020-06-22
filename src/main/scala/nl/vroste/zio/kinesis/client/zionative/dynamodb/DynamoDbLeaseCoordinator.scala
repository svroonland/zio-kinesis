package nl.vroste.zio.kinesis.client.zionative.dynamodb

import nl.vroste.zio.kinesis.client.DynamicConsumer.Record
import nl.vroste.zio.kinesis.client.zionative.LeaseCoordinator.AcquiredLease
import nl.vroste.zio.kinesis.client.zionative._
import nl.vroste.zio.kinesis.client.zionative.dynamodb.DynamoDbLeaseCoordinator.{ Lease, LeaseCommand }
import nl.vroste.zio.kinesis.client.zionative.dynamodb.DynamoDbLeaseCoordinator.LeaseCommand.{
  RenewLease,
  UpdateCheckpoint
}
import nl.vroste.zio.kinesis.client.zionative.dynamodb.LeaseTable.{
  LeaseAlreadyExists,
  LeaseObsolete,
  UnableToClaimLease
}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._
import software.amazon.awssdk.services.kinesis.model.Shard
import zio._
import zio.clock.Clock
import zio.duration._
import zio.logging._
import zio.random.{ shuffle, Random }
import zio.stream.ZStream
import scala.util.control.NoStackTrace
import nl.vroste.zio.kinesis.client.zionative.dynamodb.DynamoDbLeaseCoordinator.LeaseCommand.RefreshLease
import nl.vroste.zio.kinesis.client.zionative.dynamodb.DynamoDbLeaseCoordinator.LeaseCommand.ReleaseLease

object ZioExtensions {
  implicit class OnSuccessSyntax[R, E, A](val zio: ZIO[R, E, A]) extends AnyVal {
    final def onSuccess(cleanup: A => URIO[R, Any]): ZIO[R, E, A] =
      zio.onExit {
        case Exit.Success(a) => cleanup(a)
        case _               => ZIO.unit
      }
  }
}

// TODO use
case class LeaseCoordinationSettings(
  renewalInterval: Duration = 5.seconds,
  refreshInterval: Duration = 30.seconds,
  stealInterval: Duration = 30.seconds
)

/**
 *
 * @param currentLeases Latest known state of all leases
 * @param heldLeases Leases held by this worker including a completion signal
 * @param shards List of all of the stream's shards
 */
case class State(
  currentLeases: Map[String, Lease],
  heldLeases: Map[String, (Lease, Promise[Nothing, Unit])],
  shards: Map[String, Shard]
) {
  def updateLease(lease: Lease): State =
    copy(
      currentLeases = currentLeases + (lease.key -> lease),
      heldLeases = heldLeases ++ (heldLeases.collect {
        case (shard, (_, completed)) if shard == lease.key => (shard, (lease, completed))
      })
    )

  def updateLeases(leases: List[Lease]): State =
    copy(currentLeases = leases.map(l => l.key -> l).toMap)

  def getLease(shardId: String): Option[Lease] =
    currentLeases.get(shardId)

  // TODO make this a projection of currentLeases and heldLeases = Map[String, Promise[Nothing, Unit]] to ensure there's only 1 copy of the lease
  def getHeldLease(shardId: String): Option[(Lease, Promise[Nothing, Unit])] =
    heldLeases.get(shardId)

  def hasHeldLease(shardId: String): Boolean = heldLeases.contains(shardId)

  def holdLease(lease: Lease, completed: Promise[Nothing, Unit]): State =
    copy(heldLeases = heldLeases + (lease.key -> (lease -> completed)))

  def releaseLease(lease: Lease): State                                 =
    copy(heldLeases = heldLeases - lease.key)

}

object State {
  val empty = State(Map.empty, Map.empty, Map.empty)
}

/**
 * How it works:
 *
 * - There's a lease table with a record for every shard.
 * - The lease owner is the identifier of the  worker that has currently claimed the lease.
 *   No owner means that the previous worked has released the lease.
 *   The lease should be released (owner set to null) when the application shuts down.
 * - The checkpoint is the sequence number of the last processed record
 * - The lease counter is an atomically updated counter to prevent concurrent changes. If it's not
 *   what expected when updating, another worker has probably stolen the lease. It is updated for every
 *   change to the lease.
 * - Leases are periodically refreshed for the purposes of:
 *    1. Detecting that another worker has stolen our lease if we checkpoint at larger intervals
 *       than the lease renewal interval.
 *    2. Detecting the number of active workers as input for the lease stealing algorithm
 *    3. Detecting zombie workers / workers that left, so we can take their leases
 * - Owner switches since checkpoint? Not sure what useful for.. TODO
 * - Parent shard IDs: for resharding and proper sequential handling (not yet handled TODO)
 * - pending checkpoint: not sure what for (not yet supported TODO)
 *
 * Terminology:
 * - held lease: a lease held by this worker
 */
private class DynamoDbLeaseCoordinator(
  table: LeaseTable,
  workerId: String,
  state: Ref[State],
  acquiredLeasesQueue: Queue[(Lease, Promise[Nothing, Unit])],
  emitDiagnostic: DiagnosticEvent => UIO[Unit] = _ => UIO.unit,
  commandQueue: Queue[LeaseCommand]
) extends LeaseCoordinator {

  import DynamoDbLeaseCoordinator._
  import ZioExtensions.OnSuccessSyntax

  /**
   * Operations that update a held lease will interfere unless we run them sequentially for each shard
   */
  val runloop = {

    def leaseLost(lease: Lease, leaseCompleted: Promise[Nothing, Unit]): UIO[Unit] =
      leaseCompleted.succeed(()) *>
        state.update(_.releaseLease(lease).updateLease(lease)) *>
        emitDiagnostic(DiagnosticEvent.ShardLeaseLost(lease.key))

    def doUpdateCheckpoint(
      shard: String,
      checkpoint: ExtendedSequenceNumber,
      release: Boolean
    ) =
      for {
        heldleaseWithComplete  <- state.get
                                   .map(_.heldLeases.get(shard))
                                   .someOrFail(Right(ShardLeaseLost): Either[Throwable, ShardLeaseLost.type])
        (lease, leaseCompleted) = heldleaseWithComplete
        updatedLease            = lease.copy(
                         counter = lease.counter + 1,
                         checkpoint = Some(checkpoint),
                         owner = lease.owner.filterNot(_ => release)
                       )
        _                      <- (table
                 .updateCheckpoint(updatedLease) <* emitDiagnostic(
                 DiagnosticEvent.Checkpoint(shard, checkpoint)
               )).catchAll {
               case _: ConditionalCheckFailedException =>
                 leaseLost(updatedLease, leaseCompleted) *>
                   ZIO.fail(Right(ShardLeaseLost))
               case e                                  =>
                 ZIO.fail(Left(e))
             }.onSuccess { _ =>
               state.update(_.updateLease(updatedLease)) *>
                 (
                   leaseCompleted.succeed(()) *>
                     state.update(_.releaseLease(updatedLease)) *>
                     emitDiagnostic(DiagnosticEvent.LeaseReleased(shard))
                 ).when(release)
             }
      } yield ()

    // Lease renewal increases the counter only. May detect that lease was stolen
    def doRenewLease(shard: String) =
      state.get.map(_.getHeldLease(shard)).flatMap {
        case Some((lease, leaseCompleted)) =>
          val updatedLease = lease.increaseCounter
          for {
            _ <- table.renewLease(updatedLease).catchAll {
                   // This means the lease was updated by another worker
                   case Right(LeaseObsolete) =>
                     leaseLost(lease, leaseCompleted) *>
                       log.info(s"Unable to renew lease for shard, lease counter was obsolete").unit
                   case Left(e)              => ZIO.fail(e)
                 }
            _ <- state.update(_.updateLease(updatedLease))
            _ <- emitDiagnostic(DiagnosticEvent.LeaseRenewed(updatedLease.key))
          } yield ()
        case None                          =>
          ZIO.fail(new Exception(s"Unknown lease for shard ${shard}! This indicates a programming error"))
      }

    def doRefreshLease(lease: Lease, done: Promise[Nothing, Unit]): UIO[Unit] =
      for {
        currentState <- state.get
        shardId       = lease.key
        _            <- (currentState.getLease(shardId), currentState.getHeldLease(shardId)) match {
               // This is one of our held leases, we expect it to be unchanged
               case (Some(previousLease), Some(_)) if previousLease.counter == lease.counter =>
                 ZIO.unit
               // This is our held lease that was stolen by another worker
               case (Some(previousLease), Some((_, leaseCompleted)))                         =>
                 if (previousLease.counter != lease.counter && previousLease.owner != lease.owner)
                   leaseLost(lease, leaseCompleted)
                 else
                   // We have ourselves updated this lease by eg checkpointing or renewing
                   ZIO.unit
               // Update of a lease that we do not hold or have not yet seen
               case _                                                                        =>
                 state.update(_.updateLease(lease))
             }
      } yield ()

    def doReleaseLease(shard: String) =
      state.get.flatMap(
        _.getHeldLease(shard)
          .map(releaseHeldLease(_))
          .getOrElse(ZIO.unit)
      )

    def releaseHeldLease: ((Lease, Promise[Nothing, Unit])) => ZIO[Logging, Throwable, Unit] = {
      case (lease, completed) =>
        val updatedLease = lease.copy(owner = None).increaseCounter

        table.releaseLease(updatedLease).catchAll {
          case Right(ShardLeaseLost) => // This is fine at shutdown
            ZIO.unit
          case Left(e)               =>
            ZIO.fail(e)
        } *> completed.succeed(()) *>
          state.update(_.releaseLease(updatedLease)) *>
          emitDiagnostic(DiagnosticEvent.LeaseReleased(lease.key))
    }

    ZStream
      .fromQueue(commandQueue)
      .buffer(100)
      .groupByKey(_.shard) {
        case (shard @ _, command) =>
          command.mapM {
            case UpdateCheckpoint(shard, checkpoint, done, release) =>
              doUpdateCheckpoint(shard, checkpoint, release).foldM(done.fail, done.succeed)
            case RenewLease(shard, done)                            =>
              doRenewLease(shard).foldM(done.fail, done.succeed)
            case RefreshLease(lease, done)                          =>
              doRefreshLease(lease, done).tap(done.succeed) // Cannot fail
            case ReleaseLease(shard, done)                          =>
              doReleaseLease(shard).foldM(done.fail, done.succeed)
          }
      }
  }

  /**
   * If our application does not checkpoint, we still need to periodically renew leases, otherwise other workers
   * will think ours are expired
   */
  val renewLeases = for {
    _          <- log.info("Renewing leases")
    // TODO we could skip renewing leases that were recently checkpointed, save a few DynamoDB credits
    heldLeases <- state.get.map(_.heldLeases.keySet)
    _          <- ZIO
           .foreach_(heldLeases)(shardId => processCommand(LeaseCommand.RenewLease(shardId, _)))
  } yield ()

  /**
   * For lease taking to work properly, we need to have the latest state of leases
   *
   * Also if other workers have taken our leases and we haven't yet checkpointed, we can find out proactively
   */
  val refreshLeases = {
    for {
      _      <- log.info("Refreshing leases")
      leases <- table.getLeasesFromDB
      _      <- log.info(s"Refreshing leases found ${leases.size} leases")
      _      <- ZIO.foreach_(leases)(lease => processCommand(LeaseCommand.RefreshLease(lease, _)))
      _      <- log.info("Refreshing leases done")
    } yield ()
  }

  /**
   * Takes leases, unowned or from other workers, to get this worker's number of leases to the target value (nr leases / nr workers)
   *
   * Taking individual leases may fail, but the others will still be tried. Only in the next round of lease taking will we try again.
   */
  val takeLeases =
    for {
      _             <- log.info("Stealing leases")
      state         <- state.get
      toSteal       <- leasesToTake(state.currentLeases.values.toList, workerId)
      claimedLeases <- ZIO
                         .foreachPar(toSteal) { lease =>
                           val updatedLease = lease.claim(workerId)
                           (table.claimLease(updatedLease).as(Some(updatedLease)) <* emitDiagnostic(
                             DiagnosticEvent.LeaseStolen(lease.key, lease.owner.get)
                           )).catchAll {
                             case Right(UnableToClaimLease) =>
                               log
                                 .info(
                                   s"$workerId Unable to claim lease for shard ${lease.key}, beaten to it by another worker?"
                                 )
                                 .as(None)
                             case Left(e)                   =>
                               ZIO.fail(e)

                           }
                         }
                         .map(_.flatten)
      _             <- ZIO.foreach_(claimedLeases)(registerNewAcquiredLease)
    } yield ()

  // Puts it in the state and the queue
  private def registerNewAcquiredLease(lease: Lease): ZIO[Logging, Nothing, Unit] =
    for {
      completed <- Promise.make[Nothing, Unit]
      _         <- state.update(_.updateLease(lease).holdLease(lease, completed))
      _         <- emitDiagnostic(DiagnosticEvent.LeaseAcquired(lease.key, lease.checkpoint))
      _         <- acquiredLeasesQueue.offer(lease -> completed)
    } yield ()

  /**
   * Claims all available leases for the current shards (no owner or not existent)
   */
  def initializeLeases: ZIO[Logging, Throwable, Unit] =
    for {
      state             <- state.get
      allLeases          = state.currentLeases
      _                  =
        println(
          s"Initializing. Shards: ${state.shards.values.map(_.shardId()).mkString(",")}, found leases: ${allLeases.mkString(",")}"
        )
      // Claim new leases for the shards the database doesn't have leases for
      shardsWithoutLease =
        state.shards.values.toList.filterNot(shard => allLeases.values.map(_.key).toList.contains(shard.shardId()))
      _                 <- log.info(
             s"No leases exist yet for these shards, creating: ${shardsWithoutLease.map(_.shardId()).mkString(",")}"
           )
      _                 <- ZIO
             .foreachPar_(shardsWithoutLease) { shard =>
               val lease = Lease(
                 key = shard.shardId(),
                 owner = Some(workerId),
                 counter = 0L,
                 ownerSwitchesSinceCheckpoint = 0L,
                 checkpoint = None,
                 parentShardIds = Seq.empty
               )

               (table.createLease(lease) <* registerNewAcquiredLease(lease)).catchAll {
                 case Right(LeaseAlreadyExists) =>
                   log
                     .info(
                       s"Unable to claim lease for shard ${lease.key}, beaten to it by another worker?"
                     )
                 case Left(e)                   =>
                   ZIO.fail(e)
               }
             }
      // Claim leases without owner or ones that we left ourselves (ungraceful)
      // TODO we can also claim leases that have expired (worker crashed before releasing)
      claimableLeases    = allLeases.filter {
                          case (key @ _, lease) => lease.owner.isEmpty || lease.owner.contains(workerId)
                        }
      _                 <- log.info(s"Claim of leases without owner for shards ${claimableLeases.mkString(",")}")
      // TODO this might be somewhat redundant with lease stealing
      _                 <- ZIO
             .foreachPar_(claimableLeases) {
               case (key @ _, lease) =>
                 val updatedLease = lease.claim(workerId)
                 table
                   .claimLease(updatedLease)
                   .as(updatedLease)
                   .flatMap(registerNewAcquiredLease)
                   .catchAll {
                     case Right(UnableToClaimLease) =>
                       log
                         .info(
                           s"Unable to claim lease for shard ${lease.key}, beaten to it by another worker?"
                         )
                     case Left(e)                   =>
                       ZIO.fail(e)
                   }
             }
    } yield ()

  override def acquiredLeases: ZStream[zio.clock.Clock, Throwable, AcquiredLease]       =
    ZStream
      .fromQueue(acquiredLeasesQueue)
      .map { case (lease, complete) => AcquiredLease(lease.key, complete) }

  override def getCheckpointForShard(shard: Shard): UIO[Option[ExtendedSequenceNumber]] =
    for {
      leaseOpt <- state.get.map(_.currentLeases.get(shard.shardId()))
    } yield leaseOpt.flatMap(_.checkpoint)

  private def processCommand[E, A](makeCommand: Promise[E, A] => LeaseCommand): IO[E, A] =
    for {
      promise <- Promise.make[E, A]
      command  = makeCommand(promise)
      _       <- commandQueue.offer(command)
      result  <- promise.await
    } yield result

  override def makeCheckpointer(shard: Shard) =
    for {
      staged <- Ref.make[Option[ExtendedSequenceNumber]](None)
      permit <- Semaphore.make(1)
    } yield new Checkpointer {
      override def checkpoint: ZIO[zio.blocking.Blocking, Either[Throwable, ShardLeaseLost.type], Unit] =
        doCheckpoint()

      override def checkpointAndRelease: ZIO[zio.blocking.Blocking, Either[Throwable, ShardLeaseLost.type], Unit] =
        doCheckpoint(release = true)

      private def doCheckpoint(release: Boolean = false) =
        /**
         * This uses a semaphore to ensure that two concurrent calls to `checkpoint` do not attempt
         * to process the last staged record
         *
         * It is fine to stage new records for checkpointing though, those will be processed in the
         * next call tho `checkpoint`. Staging is therefore not protected by a semaphore.
         *
         * If a new record is staged while the checkpoint operation is busy, the next call to checkpoint
         * will use that record's sequence number.
         *
         * If checkpointing fails, the last staged sequence number is left unchanged, even it has already been
         * updated.
         *
         * TODO make a test for that: staging while checkpointing should not lose the staged checkpoint.
         */
        permit.withPermit {
          for {
            lastStaged <- staged.get
            _          <- lastStaged match {
                   case Some(checkpoint) =>
                     processCommand(LeaseCommand.UpdateCheckpoint(shard.shardId(), checkpoint, _, release = release))
                     // onSuccess to ensure updating in the face of interruption
                     // only update when the staged record has not changed while checkpointing
                       .onSuccess(_ => staged.updateSome { case Some(lastStaged) => None })
                   case None if release  =>
                     processCommand(LeaseCommand.ReleaseLease(shard.shardId(), _)).mapError(Left(_))
                   case None             =>
                     ZIO.unit
                 }
          } yield ()
        }

      override def stage(r: Record[_]): zio.UIO[Unit] =
        staged.set(Some(ExtendedSequenceNumber(r.sequenceNumber, r.subSequenceNumber)))
    }

  def releaseLeases: ZIO[Logging, Throwable, Unit] =
    state.get
      .map(_.heldLeases.values)
      .flatMap(ZIO.foreachPar_(_) { case (lease, _) => releaseLease(lease.key) })

  override def releaseLease(shard: String)         =
    processCommand(LeaseCommand.ReleaseLease(shard, _))
}

object DynamoDbLeaseCoordinator {

  /**
   * Commands relating to leases that need to be executed non-concurrently per lease, because
   * they may interfere with eachother
   **/
  sealed trait LeaseCommand {
    val shard: String
  }

  object LeaseCommand {
    case class RefreshLease(lease: Lease, done: Promise[Nothing, Unit]) extends LeaseCommand {
      val shard = lease.key
    }

    case class RenewLease(shard: String, done: Promise[Throwable, Unit]) extends LeaseCommand
    // case class StealLease(shard: String, done: Promise[Nothing, Unit])   extends Command
    case class UpdateCheckpoint(
      shard: String,
      checkpoint: ExtendedSequenceNumber,
      done: Promise[Either[Throwable, ShardLeaseLost.type], Unit],
      release: Boolean
    ) extends LeaseCommand

    case class ReleaseLease(shard: String, done: Promise[Throwable, Unit]) extends LeaseCommand

  }

  case class Lease(
    key: String,
    owner: Option[String],
    counter: Long,
    ownerSwitchesSinceCheckpoint: Long,
    checkpoint: Option[ExtendedSequenceNumber],
    parentShardIds: Seq[String],
    pendingCheckpoint: Option[ExtendedSequenceNumber] = None
  ) {
    def increaseCounter: Lease = copy(counter = counter + 1)

    def claim(owner: String): Lease =
      copy(owner = Some(owner), counter = counter + 1, ownerSwitchesSinceCheckpoint = ownerSwitchesSinceCheckpoint + 1)
  }

  def make(
    applicationName: String,
    workerId: String,
    shards: Map[String, Shard],
    emitDiagnostic: DiagnosticEvent => UIO[Unit] = _ => UIO.unit
  ): ZManaged[Clock with Random with Logging with Has[DynamoDbAsyncClient], Throwable, LeaseCoordinator] =
    ZManaged.make {
      log.locally(LogAnnotation.Name(s"worker-${workerId}" :: Nil)) {
        for {
          table          <- ZIO.service[DynamoDbAsyncClient].map(new LeaseTable(_, applicationName))
          exists         <- table.createLeaseTableIfNotExists
          currentLeases  <- if (exists) table.getLeasesFromDB else ZIO.succeed(List.empty)
          leases          = currentLeases.map(l => l.key -> l).toMap
          _              <- log.info(s"Found ${currentLeases.size} existing leases: " + currentLeases.mkString("\n"))
          state          <- Ref.make[State](State(leases, Map.empty, shards))
          acquiredLeases <- Queue.unbounded[(Lease, Promise[Nothing, Unit])]
          commandQueue   <- Queue.unbounded[LeaseCommand]

          coordinator = new DynamoDbLeaseCoordinator(
                          table,
                          workerId,
                          state,
                          acquiredLeases,
                          emitDiagnostic,
                          commandQueue
                        )
        } yield coordinator
      }
    }(_.releaseLeases.orDie).tap { c =>
      // Do all initalization in parallel
      for {
        _ <- logNamed(s"worker-${workerId}")(c.initializeLeases *> c.takeLeases).forkManaged
        _ <- logNamed(s"worker-${workerId}")(c.runloop.runDrain).forkManaged
        _ <- logNamed(s"worker-${workerId}") {
               (c.refreshLeases *> c.renewLeases *> c.takeLeases)
               // TODO all needs to be configurable
                 .repeat(Schedule.fixed(5.seconds))
                 .delay(5.seconds)
             }.forkManaged
      } yield ()
    }

  def logNamed[R, E, A](name: String)(f: ZIO[R, E, A]): ZIO[Logging with R, E, A] =
    log.locally(LogAnnotation.Name(name :: Nil))(f)

  /**
   * Compute which leases to take, either without owner or from other workers
   *
   * We take unowned leases first. We only steal if necessary and do it from the busiest worker first.
   * We will not steal more than the other worker's target (nr leases / nr workers).
   *
    * @param allLeases Latest known state of the all leases
   * @param workerId ID of this worker
   * @return List of leases that should be taken by this worker
   */
  def leasesToTake(allLeases: List[Lease], workerId: String): ZIO[Random with Logging, Nothing, List[Lease]] = {
    val allWorkers = allLeases.map(_.owner).collect { case Some(owner) => owner }.toSet ++ Set(workerId)
    val target     = Math.floor(allLeases.size * 1.0 / (allWorkers.size * 1.0)).toInt

    val ourLeases = allLeases.filter(_.owner.contains(workerId))

    val nrLeasesToTake = target - ourLeases.size
    if (nrLeasesToTake > 0)
      for {
        leasesWithoutOwner <- shuffle(allLeases.filter(_.owner.isEmpty)).map(_.take(nrLeasesToTake))
        remaining           = nrLeasesToTake - leasesWithoutOwner.size // Is never less than 0 because of the take ^^
        toSteal            <- leasesToSteal(allLeases, workerId, target, nrLeasesToSteal = remaining)
      } yield leasesWithoutOwner ++ toSteal
    else ZIO.succeed(List.empty)
  }

  /**
   * Computes leases to steal from other workers
   *
   * @param allLeases Latest known state of the all leases
   * @param workerId ID of this worker
   * @param target Target number of leases for this worker
   * @param nrLeasesToSteal How many leases to steal to get to the target
   * @return List of leases that should be stolen
   */
  def leasesToSteal(
    allLeases: List[Lease],
    workerId: String,
    target: Int,
    nrLeasesToSteal: Int
  ): ZIO[Random with Logging, Nothing, List[Lease]] = {
    val leasesByWorker = allLeases.groupBy(_.owner).collect { case (Some(owner), leases) => owner -> leases }.toMap
    val ourLeases      = allLeases.filter(_.owner.contains(workerId))
    val allWorkers     = allLeases.map(_.owner).collect { case Some(owner) => owner }.toSet ++ Set(
      workerId
    ) // We may already own some leases
    // println(
    // s"We have ${ourLeases.size}, we would like to have ${target}/${allLeases.size} leases (${allWorkers.size} workers)"
    // )
    // println(s"Planning to steal ${nrLeasesToSteal} leases")

    // From busiest to least busy
    val nrLeasesByWorker  = allWorkers
      .map(worker => worker -> allLeases.count(_.owner.contains(worker)))
      .toMap
      .view
      .filterKeys(_ != workerId)
      .toList
      .sortBy {
        case (worker, nrLeases) => (nrLeases * -1, worker)
      } // Sort desc by nr of leases and then asc by worker ID for deterministic sort order
    // println(s"Nr Leases by worker: ${nrLeasesByWorker}")
    val spilloverByWorker = nrLeasesByWorker.map {
      case (worker, nrLeases) => worker -> Math.max(0, nrLeases - target)
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

    // Candidate leases
    val candidatesByWorker = allLeases.filter(_.owner.forall(_ != workerId))

    // Steal leases
    for {
      leasesToStealByWorker <- ZIO.foreach(nrLeasesToStealByWorker) {
                                 case (worker, nrLeasesToTake) =>
                                   shuffle(leasesByWorker.get(worker).getOrElse(List.empty))
                                     .map(_.take(nrLeasesToTake))
                               }
      leasesToSteal          = leasesToStealByWorker.flatten
      _                     <- log.info(s"Going to steal ${nrLeasesToSteal} leases: ${leasesToSteal.mkString(",")}")
    } yield leasesToSteal
  }

}
