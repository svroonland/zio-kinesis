package nl.vroste.zio.kinesis.client.zionative.leasecoordinator

import java.time.Instant

import nl.vroste.zio.kinesis.client.DynamicConsumer.Record
import nl.vroste.zio.kinesis.client.zionative.LeaseCoordinator.AcquiredLease
import nl.vroste.zio.kinesis.client.zionative._
import nl.vroste.zio.kinesis.client.zionative.leasecoordinator.DefaultLeaseCoordinator.LeaseCommand.{
  RefreshLease,
  ReleaseLease,
  RenewLease,
  UpdateCheckpoint
}
import nl.vroste.zio.kinesis.client.zionative.leasecoordinator.DefaultLeaseCoordinator.{ Lease, LeaseCommand }
import nl.vroste.zio.kinesis.client.zionative.LeaseRepository.{ LeaseAlreadyExists, LeaseObsolete, UnableToClaimLease }
import software.amazon.awssdk.services.kinesis.model.Shard
import zio._
import zio.clock.Clock
import zio.duration._
import zio.logging._
import zio.random.{ shuffle, Random }
import zio.stream.ZStream
import scala.collection.compat._

object ZioExtensions {
  implicit class OnSuccessSyntax[R, E, A](val zio: ZIO[R, E, A]) extends AnyVal {
    final def onSuccess(cleanup: A => URIO[R, Any]): ZIO[R, E, A] =
      zio.onExit {
        case Exit.Success(a) => cleanup(a)
        case _               => ZIO.unit
      }
  }
}

/**
 * Default values are compatible with KCL defaults (TODO not quite yet)
 *
 * @param expirationTime Time after which a lease is considered expired if not updated in the meantime
 * @param refreshAndTakeInterval Interval at which leases are refreshed and possibly new leases taken
 * @param renewInterval Interval at which leases are renewed to prevent them expiring
 * @param maxParallelLeaseAcquisitions Maximum parallel calls to DynamoDB to claim a lease.
 *                                     This is not the maximum number of leases to steal in one iteration
 *                                     of `refreshAndTakeInterval`.
 * @param maxParallelLeaseRenewals Maximum parallel calls to DynamoDB to claim a lease.
 *                                     This is not the maximum number of leases to steal in one iteration
 *                                     of `refreshAndTakeInterval`.
 */
case class LeaseCoordinationSettings(
  expirationTime: Duration = 10.seconds,
  renewInterval: Duration = 3.seconds,
  refreshAndTakeInterval: Duration = 20.seconds,
  maxParallelLeaseAcquisitions: Int = 10,
  maxParallelLeaseRenewals: Int = 10
) {
  require(renewInterval < expirationTime, "renewInterval must be less than expirationTime")
}

case class LeaseState(lease: Lease, completed: Option[Promise[Nothing, Unit]], lastUpdated: Instant) {
  def update(updatedLease: Lease, now: Instant) =
    copy(lease = updatedLease, lastUpdated = if (updatedLease.counter > lease.counter) now else lastUpdated)
}

/**
 *
 * @param currentLeases Latest known state of all leases
 * @param heldLeases Leases held by this worker including a completion signal
 * @param shards List of all of the stream's shards
 */
case class State(
  currentLeases: Map[String, LeaseState],
  shards: Map[String, Shard]
) {

  val heldLeases = currentLeases.collect {
    case (shard, LeaseState(lease, Some(completed), _)) => shard -> (lease -> completed)
  }

  def updateShards(shards: Map[String, Shard]): State = copy(shards = shards)

  def updateLease(lease: Lease, now: Instant): State =
    updateLeases(List(lease), now)

  def updateLeases(leases: List[Lease], now: Instant): State =
    copy(currentLeases =
      currentLeases ++ leases
        .map(l =>
          l.key -> currentLeases
            .get(l.key)
            .map(_.update(l, now))
            .getOrElse(LeaseState(l, None, now))
        )
        .toMap
    )

  def getLease(shardId: String): Option[Lease] =
    currentLeases.get(shardId).map(_.lease)

  def getHeldLease(shardId: String): Option[(Lease, Promise[Nothing, Unit])] =
    heldLeases.get(shardId)

  def hasHeldLease(shardId: String): Boolean = heldLeases.contains(shardId)

  def holdLease(lease: Lease, completed: Promise[Nothing, Unit], now: Instant): State =
    copy(currentLeases =
      currentLeases + (lease.key -> currentLeases
        .get(lease.key)
        .map(_.copy(lease = lease, completed = Some(completed), lastUpdated = now))
        .getOrElse(LeaseState(lease, Some(completed), now)))
    )

  def releaseLease(lease: Lease, now: Instant): State =
    copy(currentLeases =
      currentLeases + (lease.key -> currentLeases
        .get(lease.key)
        .map(_.copy(lease = lease, completed = None, lastUpdated = now))
        .get)
    )

}

object State {
  val empty = State(Map.empty, Map.empty)

  def make(leases: List[Lease], shards: Map[String, Shard]): ZIO[Clock, Nothing, State] =
    clock.currentDateTime.orDie.map(_.toInstant()).map { now =>
      State(leases.map(l => l.key -> LeaseState(l, None, now)).toMap, shards)
    }

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
private class DefaultLeaseCoordinator(
  table: LeaseRepository.Service,
  workerId: String,
  state: Ref[State],
  acquiredLeasesQueue: Queue[(Lease, Promise[Nothing, Unit])],
  emitDiagnostic: DiagnosticEvent => UIO[Unit] = _ => UIO.unit,
  commandQueue: Queue[LeaseCommand],
  settings: LeaseCoordinationSettings
) extends LeaseCoordinator {

  import DefaultLeaseCoordinator._
  import ZioExtensions.OnSuccessSyntax

  val now = zio.clock.currentDateTime.map(_.toInstant())

  private def updateState(f: (State, Instant) => State) = now.flatMap(n => state.update(f(_, n)))

  private def updateShards(shards: Map[String, Shard]): UIO[Unit] =
    state.update(_.updateShards(shards))

  /**
   * Operations that update a held lease will interfere unless we run them sequentially for each shard
   */
  val runloop = {

    def leaseLost(lease: Lease, leaseCompleted: Promise[Nothing, Unit]): ZIO[Clock, Throwable, Unit] =
      leaseCompleted.succeed(()) *>
        updateState((s, now) => s.releaseLease(lease, now).updateLease(lease, now)) *>
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
               case Right(LeaseObsolete) =>
                 leaseLost(updatedLease, leaseCompleted).orDie *>
                   ZIO.fail(Right(ShardLeaseLost))
               case Left(e)              =>
                 ZIO.fail(Left(e))
             }.onSuccess { _ =>
               (updateState(_.updateLease(updatedLease, _)) *>
                 (
                   leaseCompleted.succeed(()) *>
                     updateState(_.releaseLease(updatedLease, _)) *>
                     emitDiagnostic(DiagnosticEvent.LeaseReleased(shard))
                 ).when(release)).orDie
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
                   case Left(e)              =>
                     // TODO we should probably retry this
                     ZIO.fail(e)
                 }
            _ <- updateState(_.updateLease(updatedLease, _))
            _ <- emitDiagnostic(DiagnosticEvent.LeaseRenewed(updatedLease.key))
          } yield ()
        case None                          =>
          ZIO.fail(new Exception(s"Unknown lease for shard ${shard}! This indicates a programming error"))
      }

    def doRefreshLease(lease: Lease): ZIO[Clock, Throwable, Unit] =
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
                 updateState(_.updateLease(lease, _))
             }
      } yield ()

    def doReleaseLease(shard: String) =
      state.get.flatMap(
        _.getHeldLease(shard)
          .map(releaseHeldLease(_))
          .getOrElse(ZIO.unit)
      )

    def releaseHeldLease: ((Lease, Promise[Nothing, Unit])) => ZIO[Logging with Clock, Throwable, Unit] = {
      case (lease, completed) =>
        val updatedLease = lease.copy(owner = None).increaseCounter

        table.releaseLease(updatedLease).catchAll {
          case Right(LeaseObsolete) => // This is fine at shutdown
            ZIO.unit
          case Left(e)              =>
            ZIO.fail(e)
        } *> completed.succeed(()) *>
          updateState(_.releaseLease(updatedLease, _)) *>
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
              doRefreshLease(lease).tap(done.succeed) // Cannot fail
            case ReleaseLease(shard, done)                          =>
              doReleaseLease(shard).foldM(done.fail, done.succeed)
          }
      }
  }

  /**
   * If our application does not checkpoint, we still need to periodically renew leases, otherwise other workers
   * will think ours are expired
   */
  val renewLeases: ZIO[Any, Throwable, Unit] = for {
    // TODO we could skip renewing leases that were recently checkpointed, save a few DynamoDB credits
    heldLeases <- state.get.map(_.heldLeases.keySet)
    _          <- ZIO
           .foreachParN_(settings.maxParallelLeaseRenewals)(heldLeases)(shardId =>
             processCommand(LeaseCommand.RenewLease(shardId, _))
           )
  } yield ()

  /**
   * For lease taking to work properly, we need to have the latest state of leases
   *
   * Also if other workers have taken our leases and we haven't yet checkpointed, we can find out proactively
   */
  val refreshLeases = {
    for {
      _             <- log.info("Refreshing leases")
      currentLeases <- state.get.map(_.currentLeases.values.map(_.lease))
      currentWorkers = currentLeases.map(_.owner).collect { case Some(owner) => owner }.toSet
      leases        <- table.getLeases
      _             <- ZIO.foreachPar_(leases)(lease => processCommand(LeaseCommand.RefreshLease(lease, _)))
      newWorkers     = leases.map(_.owner).collect { case Some(owner) => owner }.toSet
      workersJoined  = newWorkers -- currentWorkers
      workersLeft    = currentWorkers -- newWorkers
      _             <- ZIO.foreach(workersJoined)(w => emitDiagnostic(DiagnosticEvent.WorkerJoined(w)))
      _             <- ZIO.foreach(workersLeft)(w => emitDiagnostic(DiagnosticEvent.WorkerLeft(w))) // TODO also for zombie workers
    } yield ()
  }

  /**
   * Claims all available leases for the current shards (no owner or not existent)
   */
  val claimLeasesForShardsWithoutLease: ZIO[Logging with Clock, Throwable, Unit] =
    for {
      state             <- state.get
      allLeases          = state.currentLeases.view.mapValues(_.lease).toMap
      _                 <- log.info(s"Found ${allLeases.size} leases")
      // Claim new leases for the shards the database doesn't have leases for
      shardsWithoutLease =
        state.shards.values.toList.filterNot(shard => allLeases.values.map(_.key).toList.contains(shard.shardId()))
      _                 <-
        log
          .info(
            s"No leases exist yet for these shards, creating and claiming: ${shardsWithoutLease.map(_.shardId()).mkString(",")}"
          )
          .when(shardsWithoutLease.nonEmpty)
      _                 <- ZIO
             .foreachParN_(settings.maxParallelLeaseAcquisitions)(shardsWithoutLease) { shard =>
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
    } yield ()

  /**
   * Takes leases, unowned or from other workers, to get this worker's number of leases to the target value (nr leases / nr workers)
   *
   * Taking individual leases may fail, but the others will still be tried. Only in the next round of lease taking will we try again.
   */
  val takeLeases =
    for {
      _             <- claimLeasesForShardsWithoutLease
      state         <- state.get
      expiredLeases <- now.map(now =>
                         state.currentLeases.values
                           .filter(_.lastUpdated.isBefore(now.minusMillis(settings.expirationTime.toMillis)))
                           .map(_.lease)
                           .toList
                       )
      toTake        <- leasesToTake(state.currentLeases.map(_._2.lease).toList, workerId, expiredLeases)
      _             <- log.info(s"Going to take ${toTake.size} leases: ${toTake.mkString(",")}").when(toTake.nonEmpty)
      claimedLeases <- ZIO
                         .foreachParN(settings.maxParallelLeaseAcquisitions)(toTake) { lease =>
                           val updatedLease = lease.claim(workerId)
                           table
                             .claimLease(updatedLease)
                             .as(Some(updatedLease))
                             .catchAll {
                               case Right(UnableToClaimLease) =>
                                 log
                                   .info(
                                     s"Unable to claim lease for shard ${lease.key}, beaten to it by another worker?"
                                   )
                                   .as(None)
                               case Left(e)                   =>
                                 log.error(s"Got error ${e}") *> ZIO.fail(e)
                             }
                         }
                         .map(_.flatten)
      _             <- ZIO.foreachPar_(claimedLeases)(registerNewAcquiredLease)
    } yield ()

  // Puts it in the state and the queue
  private def registerNewAcquiredLease(lease: Lease): ZIO[Clock with Logging, Nothing, Unit] =
    for {
      completed <- Promise.make[Nothing, Unit]
      _         <- updateState((s, now) => s.updateLease(lease, now).holdLease(lease, completed, now)).orDie
      _         <- emitDiagnostic(DiagnosticEvent.LeaseAcquired(lease.key, lease.checkpoint))
      _         <- acquiredLeasesQueue.offer(lease -> completed)
    } yield ()

  /**
   * For leases that we found in the lease table that have our worker ID as owner but we don't
   * currently have in our state as acquired
   */
  val resumeUnreleasedLeases = for {
    s             <- state.get
    leasesToResume = s.currentLeases.collect {
                       case (shardId @ _, LeaseState(lease, None, _)) if lease.owner.contains(workerId) => lease
                     }
    _             <- ZIO.foreachPar_(leasesToResume)(registerNewAcquiredLease)
  } yield ()

  override def acquiredLeases: ZStream[zio.clock.Clock, Throwable, AcquiredLease]          =
    ZStream
      .fromQueue(acquiredLeasesQueue)
      .map { case (lease, complete) => AcquiredLease(lease.key, complete) }

  override def getCheckpointForShard(shardId: String): UIO[Option[ExtendedSequenceNumber]] =
    for {
      leaseOpt <- state.get.map(_.currentLeases.get(shardId))
    } yield leaseOpt.flatMap(_.lease.checkpoint)

  private def processCommand[E, A](makeCommand: Promise[E, A] => LeaseCommand): IO[E, A] =
    for {
      promise <- Promise.make[E, A]
      command  = makeCommand(promise)
      _       <- commandQueue.offer(command)
      result  <- promise.await
    } yield result

  override def makeCheckpointer(shardId: String) =
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
                     processCommand(LeaseCommand.UpdateCheckpoint(shardId, checkpoint, _, release = release))
                     // onSuccess to ensure updating in the face of interruption
                     // only update when the staged record has not changed while checkpointing
                       .onSuccess(_ => staged.updateSome { case Some(lastStaged @ _) => None })
                   case None if release  =>
                     processCommand(LeaseCommand.ReleaseLease(shardId, _)).mapError(Left(_))
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

object DefaultLeaseCoordinator {

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
    emitDiagnostic: DiagnosticEvent => UIO[Unit] = _ => UIO.unit,
    settings: LeaseCoordinationSettings,
    shards: Task[Map[String, Shard]]
  ): ZManaged[Clock with Random with Logging with LeaseRepositoryFactory, Throwable, LeaseCoordinator] =
    Queue
      .unbounded[(Lease, Promise[Nothing, Unit])]
      .toManaged(_.shutdown)
      .flatMap { acquiredLeases =>
        ZManaged.make {
          log.locally(LogAnnotation.Name(s"worker-${workerId}" :: Nil)) {
            for {
              table            <- ZIO.service[LeaseRepository.Factory].map(_.make(applicationName))
              leaseTableExists <- table.createLeaseTableIfNotExists
              state            <- Ref.make(State.empty)
              commandQueue     <- Queue.unbounded[LeaseCommand]

              coordinator = new DefaultLeaseCoordinator(
                              table,
                              workerId,
                              state,
                              acquiredLeases,
                              emitDiagnostic,
                              commandQueue,
                              settings
                            )
            } yield (coordinator, leaseTableExists)
          }
        }(_._1.releaseLeases.orDie)
      }
      .flatMap {
        case (c, leaseTableExists) =>
          // Do all initalization in parallel
          for {
            _ <- logNamed(s"worker-${workerId}")(c.runloop.runDrain).forkManaged
            // Wait for shards if the lease table does not exist yet, otherwise we assume there's leases
            // for all shards already, so just fork it. If there's new shards, the next `takeLeases` will
            // claim leases for them.
            _ <- logNamed(s"worker-${workerId}") {
                   val awaitAndUpdateShards = shards.flatMap(c.updateShards)

                   (c.refreshLeases *> c.resumeUnreleasedLeases).when(leaseTableExists) *>
                     (if (leaseTableExists) awaitAndUpdateShards.fork else awaitAndUpdateShards)
                 }.toManaged_
            _  = log.info("Begin first initialisation: takeLeases")
            _ <- logNamed(s"worker-${workerId}")(
                   // Initialization
                   (c.takeLeases) *>
                     // Periodic refresh
                     (c.refreshLeases *> c.takeLeases)
                       .repeat(
                         Schedule
                           .fixed(settings.refreshAndTakeInterval)
                           .jittered(0.95, 1.05)
                       )
                       .delay(settings.refreshAndTakeInterval)
                       .tapCause(e => log.error("Refresh & take leases failed, will retry", e))
                       .retry(Schedule.forever)
                 ).forkManaged
            _ <- logNamed(s"worker-${workerId}")(
                   c.renewLeases
                     .repeat(
                       Schedule
                         .fixed(settings.renewInterval)
                         .jittered(0.95, 1.05)
                     )
                     .delay(settings.renewInterval)
                     .tapCause(e => log.error("Renewing leases failed, will retry", e))
                     .retry(Schedule.forever)
                 ).forkManaged
          } yield c
      }

  def logNamed[R, E, A](name: String)(f: ZIO[R, E, A]): ZIO[Logging with R, E, A] =
    log.locally(LogAnnotation.Name(name :: Nil))(f)

  /**
   * Compute which leases to take, either without owner or from other workers
   *
   * We take expired and unowned leases first. We only steal if necessary and do it from the busiest worker first.
   * We will not steal more than the other worker's target (nr leases / nr workers).
   *
    * @param allLeases Latest known state of the all leases
   * @param workerId ID of this worker
   * @param expiredLeases Leases that have expired
   * @return List of leases that should be taken by this worker
   */
  def leasesToTake(
    allLeases: List[Lease],
    workerId: String,
    expiredLeases: List[Lease] = List.empty
  ): ZIO[Random with Logging, Nothing, List[Lease]] = {
    val allWorkers    = allLeases.map(_.owner).collect { case Some(owner) => owner }.toSet ++ Set(workerId)
    val activeWorkers =
      (allLeases.toSet -- expiredLeases).map(_.owner).collect { case Some(owner) => owner }.toSet ++ Set(workerId)
    val zombieWorkers = allWorkers -- activeWorkers

    val minTarget = Math.floor(allLeases.size * 1.0 / (activeWorkers.size * 1.0)).toInt

    // If the nr of workers does not evenly divide the shards, there's some leases that at least one worker should take
    // These we will not steal, only take
    val optional = allLeases.size % activeWorkers.size

    val target = minTarget

    val ourLeases = allLeases.filter(_.owner.contains(workerId))

    val minNrLeasesToTake = Math.max(0, target - ourLeases.size)
    val maxNrLeasesToTake = Math.max(0, target + optional - ourLeases.size)

    // We may already own some leases
    log.info(
      s"We have ${ourLeases.size}, we would like to have at least ${target}/${allLeases.size} leases (${activeWorkers.size} active workers, " +
        s"${zombieWorkers.size} zombie workers), we need ${minNrLeasesToTake} more with an optional ${optional}"
    ) *> (if (minNrLeasesToTake > 0)
            for {
              leasesWithoutOwner         <- shuffle(allLeases.filter(_.owner.isEmpty)).map(_.take(maxNrLeasesToTake))
              leasesExpired              <- shuffle(expiredLeases).map(_.take(maxNrLeasesToTake - leasesWithoutOwner.size))
              leasesWithoutOwnerOrExpired = leasesWithoutOwner ++ leasesExpired

              // We can only steal from our target budget, not the optional ones
              remaining = Math.max(0, minNrLeasesToTake - leasesWithoutOwnerOrExpired.size)
              _         = println(s"Remaining: ${remaining}, ${minNrLeasesToTake} to ${maxNrLeasesToTake}")
              toSteal  <- leasesToSteal(allLeases, workerId, target, nrLeasesToSteal = remaining)
            } yield leasesWithoutOwnerOrExpired ++ toSteal
          else ZIO.succeed(List.empty))
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
    val allWorkers     = allLeases.map(_.owner).collect { case Some(owner) => owner }.toSet ++ Set(workerId)
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

    // From each worker that we want to take some leases, randomize the leases to reduce contention
    for {
      leasesToStealByWorker <- ZIO.foreach(nrLeasesToStealByWorker) {
                                 case (worker, nrLeasesToTake) =>
                                   shuffle(leasesByWorker.get(worker).getOrElse(List.empty))
                                     .map(_.take(nrLeasesToTake))
                               }
      leasesToSteal          = leasesToStealByWorker.flatten
    } yield leasesToSteal
  }

}
