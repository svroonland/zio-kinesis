package nl.vroste.zio.kinesis.client.zionative.leasecoordinator

import java.time.{ DateTimeException, Instant }

import nl.vroste.zio.kinesis.client.zionative.LeaseCoordinator.AcquiredLease
import nl.vroste.zio.kinesis.client.zionative._
import nl.vroste.zio.kinesis.client.zionative.LeaseRepository.{
  Lease,
  LeaseAlreadyExists,
  LeaseObsolete,
  UnableToClaimLease
}
import software.amazon.awssdk.services.kinesis.model.Shard
import zio._
import zio.clock.Clock
import zio.duration._
import zio.logging._
import zio.random.Random
import zio.stream.ZStream
import DefaultLeaseCoordinator.State
import nl.vroste.zio.kinesis.client.zionative.Consumer.InitialPosition
import nl.vroste.zio.kinesis.client.zionative.leasecoordinator.ZioExtensions.foreachParNUninterrupted_
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException

/**
 * Coordinates leases for shards between different workers
 *
 * How it works:
 *
 * - There's a lease table with a record for every shard.
 * - The lease owner is the identifier of the worker that has currently claimed the lease.
 *   No owner means that the previous worked has released the lease.
 *   The lease is released (owner set to null) when the application shuts down.
 * - The checkpoint is the sequence number of the last processed record
 * - The lease counter is an atomically updated counter to prevent concurrent changes. If it's not
 *   what expected when updating, another worker has probably stolen the lease. It is updated for every
 *   change to the lease.
 * - Leases are periodically refreshed for the purposes of:
 *    1. Detecting that another worker has stolen our lease if we checkpoint at larger intervals
 *       than the lease renewal interval.
 *    2. Detecting the number of active workers as input for the lease stealing algorithm
 *    3. Detecting zombie workers / workers that left, so we can take their leases
 * - Parent shard IDs: when the shard was created by resharding, which shard(s) is its predecessor
 *
 * Terminology:
 * - held lease: a lease held by this worker
 */
private class DefaultLeaseCoordinator(
  table: LeaseRepository.Service,
  applicationName: String,
  workerId: String,
  state: Ref[State],
  acquiredLeasesQueue: Queue[(Lease, Promise[Nothing, Unit])],
  emitDiagnostic: DiagnosticEvent => UIO[Unit] = _ => UIO.unit,
  serialExecutionByShard: SerialExecution[String],
  settings: LeaseCoordinationSettings,
  strategy: ShardAssignmentStrategy,
  initialPosition: InitialPosition
) extends LeaseCoordinator {

  import DefaultLeaseCoordinator._
  import ZioExtensions.OnSuccessSyntax

  val now = zio.clock.currentDateTime.map(_.toInstant())

  override def updateShards(shards: Map[String, Shard]): UIO[Unit] =
    updateStateWithDiagnosticEvents(_.updateShards(shards))

  override def childShardsDetected(
    childShards: Seq[Shard]
  ): ZIO[Clock with Logging with Random, Throwable, Unit] =
    updateStateWithDiagnosticEvents(s =>
      s.updateShards(s.shards ++ childShards.map(shard => shard.shardId() -> shard).toMap)
    )

  private def updateStateWithDiagnosticEvents(f: (State, Instant) => State): ZIO[Clock, DateTimeException, Unit] =
    now.flatMap(now => updateStateWithDiagnosticEvents(f(_, now)))

  private def updateStateWithDiagnosticEvents(f: State => State): UIO[Unit] =
    for {
      stateBeforeAndAfter      <- state.modify { s =>
                               val newState = f(s)
                               ((s, newState), newState)
                             }
      (stateBefore, stateAfter) = stateBeforeAndAfter
      _                        <- emitWorkerPoolDiagnostics(
             stateBefore.currentLeases.map(_._2.lease),
             stateAfter.currentLeases.map(_._2.lease)
           )
      newShards                 = (stateAfter.shards.keySet -- stateBefore.shards.keySet).filter(_ => stateBefore.shards.nonEmpty)
      removedShards             = stateBefore.shards.keySet -- stateAfter.shards.keySet
      _                        <- ZIO.foreach_(newShards)(shardId => emitDiagnostic(DiagnosticEvent.NewShardDetected(shardId)))
      _                        <- ZIO.foreach_(removedShards)(shardId => emitDiagnostic(DiagnosticEvent.ShardEnded(shardId)))
    } yield ()

  // Emits WorkerJoined and WorkerLeft after each lease operation
  private def emitWorkerPoolDiagnostics(currentLeases: Iterable[Lease], newLeases: Iterable[Lease]): UIO[Unit] = {
    val currentWorkers = currentLeases.map(_.owner).collect { case Some(owner) => owner }.toSet
    val newWorkers     = newLeases.map(_.owner).collect { case Some(owner) => owner }.toSet
    val workersJoined  = newWorkers -- currentWorkers
    val workersLeft    = currentWorkers -- newWorkers

    for {
      _ <- ZIO.foreach_(workersJoined)(w => emitDiagnostic(DiagnosticEvent.WorkerJoined(w)))
      _ <- ZIO.foreach_(workersLeft)(w => emitDiagnostic(DiagnosticEvent.WorkerLeft(w)))
    } yield ()
  }

  def leaseLost(lease: Lease, leaseCompleted: Promise[Nothing, Unit]): ZIO[Clock, Throwable, Unit] =
    leaseCompleted.succeed(()) *>
      updateStateWithDiagnosticEvents((s, now) => s.releaseLease(lease, now).updateLease(lease.release, now)) *>
      emitDiagnostic(DiagnosticEvent.ShardLeaseLost(lease.key))

  def releaseLease(shard: String) =
    state.get.flatMap(
      _.getHeldLease(shard)
        .map(releaseHeldLease(_))
        .getOrElse(ZIO.unit)
    )

  def releaseHeldLease: ((Lease, Promise[Nothing, Unit])) => ZIO[Logging with Clock, Throwable, Unit] = {
    case (lease, completed) =>
      val updatedLease = lease.copy(owner = None).increaseCounter

      table
        .releaseLease(applicationName, updatedLease)
        .timeout(settings.releaseLeaseTimeout)
        .tap(result => log.warn(s"Timeout while releasing lease for shard ${lease.key}, ignored").when(result.isEmpty))
        .tapError {
          case Right(LeaseObsolete) => // This is fine at shutdown
            ZIO.unit
          case Left(e)              =>
            log.error(s"Error releasing lease for shard ${lease.key}, ignored: ${e}")
        }
        .ignore *>
        completed.succeed(()) *>
        updateStateWithDiagnosticEvents(_.releaseLease(updatedLease, _)) *>
        emitDiagnostic(DiagnosticEvent.LeaseReleased(lease.key))
  }

  /**
   * Increases the lease counter of held leases. Other workers can observe this and know that this worker
   * is still active.
   *
   * Leases that recently had their checkpoint updated are not updated here to save on DynamoDB usage
   */
  val renewLeases: ZIO[Clock with Logging, Throwable, Unit] = for {
    currentLeases <- state.get.map(_.currentLeases)
    now           <- now
    leasesToRenew  = currentLeases.view.collect {
                      case (shard, leaseState)
                          if !leaseState.wasUpdatedLessThan(settings.renewInterval, now) && leaseState.lease.owner
                            .contains(workerId) =>
                        shard
                    }
    _             <- log.debug(s"Renewing ${leasesToRenew.size} leases")
    _             <- foreachParNUninterrupted_(settings.maxParallelLeaseRenewals)(leasesToRenew) { shardId =>
           serialExecutionByShard(shardId)(renewLease(shardId))
             .tapError(e => log.error(s"Error renewing lease: ${e}"))
             .retry(settings.renewRetrySchedule) orElse (
             log.warn(s"Failed to renew lease for shard ${shardId}, releasing") *>
               serialExecutionByShard(shardId)(releaseLease(shardId))
           )
         }
    // _             <- log.debug(s"Renewing ${leasesToRenew.size} leases done")
  } yield ()

  // Lease renewal increases the counter only. May detect that lease was stolen
  private def renewLease(shard: String): ZIO[Logging with Clock, Throwable, Unit] =
    state.get.map(_.getHeldLease(shard)).flatMap {
      case Some((lease, leaseCompleted)) =>
        val updatedLease = lease.increaseCounter
        (for {
          duration <- table
                        .renewLease(applicationName, updatedLease)
                        .timed
                        .map(_._1)
          _        <- updateStateWithDiagnosticEvents(_.updateLease(updatedLease, _)).mapError(Left(_))
          _        <- emitDiagnostic(DiagnosticEvent.LeaseRenewed(updatedLease.key, duration))
        } yield ()).catchAll {
          // This means the lease was updated by another worker
          case Right(LeaseObsolete) =>
            leaseLost(lease, leaseCompleted) *>
              log.info(s"Unable to renew lease for shard, lease counter was obsolete")
          case Left(e)              =>
            ZIO.fail(e)
        }
      case None                          =>
        // Possible race condition between releasing the lease and the renewLeases cycle
        ZIO.fail(new Exception(s"Unknown lease for shard ${shard}! Perhaps the lease was released simultaneously"))
    }

  /**
   * For lease taking to work properly, we need to have the latest state of leases
   *
   * Also if other workers have taken our leases and we haven't yet checkpointed, we can find out now
   */
  val refreshLeases: ZIO[Logging with Clock, Throwable, Unit] =
    for {
      _        <- log.info("Refreshing leases")
      duration <- table
                    .getLeases(applicationName)
                    .mapChunksM(
                      ZIO
                        .foreachPar_(_) { lease =>
                          log.info(s"RefreshLeases: ${lease}") *>
                            serialExecutionByShard(lease.key)(refreshLease(lease))
                        }
                        .as(Chunk.unit)
                    )
                    .runDrain
                    .timed
                    .map(_._1)
      _        <- log.debug(s"Refreshing leases took ${duration.toMillis}")
    } yield ()

  private def refreshLease(lease: Lease): ZIO[Clock, Throwable, Unit] =
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
             // Lease that is still owned by us but is not actively held
             case (_, None) if lease.owner.contains(workerId)                              =>
               updateStateWithDiagnosticEvents(_.updateLease(lease, _)) *> registerNewAcquiredLease(lease)
             // Update of a lease that we do not hold or have not yet seen
             case _                                                                        =>
               updateStateWithDiagnosticEvents(_.updateLease(lease, _))
           }
    } yield ()

  private def registerNewAcquiredLease(lease: Lease): ZIO[Clock, Nothing, Unit] =
    for {
      completed <- Promise.make[Nothing, Unit]
      _         <- updateStateWithDiagnosticEvents((s, now) => s.updateLease(lease, now).holdLease(lease, completed, now)).orDie
      _         <- emitDiagnostic(DiagnosticEvent.LeaseAcquired(lease.key, lease.checkpoint))
      _         <- acquiredLeasesQueue.offer(lease -> completed)
    } yield ()

  /**
   * Claims all available leases for the current shards (no owner or not existent)
   */
  private def claimLeasesForShardsWithoutLease(
    desiredShards: Set[String],
    shards: Map[String, Shard],
    leases: Map[String, Lease]
  ): ZIO[Logging with Clock, Throwable, Unit] =
    for {
      _                 <- log.info(s"Found ${leases.size} leases")
      shardsWithoutLease = desiredShards.filterNot(leases.contains).toSeq.sorted.map(shards(_))
      _                 <- log
             .info(
               s"No leases exist yet for these shards, creating and claiming: " +
                 s"${shardsWithoutLease.map(_.shardId()).mkString(",")}"
             )
             .when(shardsWithoutLease.nonEmpty)
      _                 <- ZIO
             .foreachParN_(settings.maxParallelLeaseAcquisitions)(shardsWithoutLease) { shard =>
               val lease = Lease(
                 key = shard.shardId(),
                 owner = Some(workerId),
                 counter = 0L,
                 checkpoint = Some(Left(initialCheckpointForShard(shard, initialPosition, leases))),
                 parentShardIds = shard.parentShardIds
               )

               (table.createLease(applicationName, lease) <* serialExecutionByShard(lease.key)(
                 registerNewAcquiredLease(lease)
               )).catchAll {
                 case Right(LeaseAlreadyExists) =>
                   log.info(s"Unable to claim lease for shard ${lease.key}, beaten to it by another worker?")
                 case Left(e)                   =>
                   log.error(s"Error creating lease: ${e}") *> ZIO.fail(e)
               }
             }
    } yield ()

  /**
   * Takes leases, unowned or from other workers, to get this worker's number of leases to the target value (nr leases / nr workers)
   *
   * Taking individual leases may fail, but the others will still be tried. Only in the next round of lease taking will we try again.
   *
   * The effect fails with a Throwable when having failed to take one or more leases
   */
  val takeLeases: ZIO[Clock with Logging with Random, Throwable, Unit] = {
    for {
      leases          <- state.get.map(_.currentLeases.values.toSet)
      shards          <- state.get.map(_.shards)
      leaseMap         = leases.map(s => s.lease.key -> s.lease).toMap
      openLeases       = leases.collect {
                     case LeaseState(lease, completed @ _, lastUpdated) if !shardHasEnded(lease) =>
                       (lease, lastUpdated)
                   }
      consumableShards = shardsReadyToConsume(shards, leaseMap)
      desiredShards   <- strategy.desiredShards(openLeases, consumableShards.keySet, workerId)
      _               <- log.info(s"Desired shard assignment: ${desiredShards.mkString(",")}")
      toTake           = leaseMap.values.filter(l => desiredShards.contains(l.key) && !l.owner.contains(workerId))
      _               <- claimLeasesForShardsWithoutLease(desiredShards, shards, leaseMap)
      _               <- log
             .info(s"Going to take ${toTake.size} leases from other workers: ${toTake.mkString(",")}")
             .when(toTake.nonEmpty)
      _               <- foreachParNUninterrupted_(settings.maxParallelLeaseAcquisitions)(toTake) { lease =>
             val updatedLease = lease.claim(workerId)
             (table.claimLease(applicationName, updatedLease) *>
               serialExecutionByShard(updatedLease.key)(registerNewAcquiredLease(updatedLease))).catchAll {
               case Right(UnableToClaimLease) =>
                 log
                   .info(
                     s"Unable to claim lease for shard ${lease.key}, beaten to it by another worker?"
                   )
               case Left(e)                   =>
                 log.error(s"Got error ${e}") *> ZIO.fail(e)
             }
           }
    } yield ()
  }

  override def acquiredLeases: ZStream[zio.clock.Clock, Throwable, AcquiredLease]                                     =
    ZStream
      .fromQueue(acquiredLeasesQueue)
      .map { case (lease, complete) => AcquiredLease(lease.key, complete) }

  override def getCheckpointForShard(shardId: String): UIO[Option[Either[SpecialCheckpoint, ExtendedSequenceNumber]]] =
    for {
      leaseOpt <- state.get.map(_.currentLeases.get(shardId))
    } yield leaseOpt.flatMap(_.lease.checkpoint)

  override def makeCheckpointer(shardId: String) =
    for {
      state  <- Ref.make(DefaultCheckpointer.State.empty)
      permit <- Semaphore.make(1)
      env    <- ZIO.environment[Logging with Clock with Random]
    } yield new DefaultCheckpointer(
      shardId,
      env,
      state,
      permit,
      (checkpoint, release) =>
        serialExecutionByShard(shardId)(updateCheckpoint(shardId, checkpoint, release).provide(env)),
      serialExecutionByShard(shardId)(releaseLease(shardId).provide(env))
    )

  private def updateCheckpoint(
    shard: String,
    checkpoint: Either[SpecialCheckpoint, ExtendedSequenceNumber],
    release: Boolean
  ): ZIO[Logging with Clock with Random, Either[Throwable, ShardLeaseLost.type], Unit] =
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
      shardEnded              = checkpoint == Left(SpecialCheckpoint.ShardEnd)
      _                      <- (table.updateCheckpoint(applicationName, updatedLease) <*
               emitDiagnostic(DiagnosticEvent.Checkpoint(shard, checkpoint))).catchAll {
             case Right(LeaseObsolete) =>
               leaseLost(updatedLease, leaseCompleted).orDie *>
                 ZIO.fail(Right(ShardLeaseLost))
             case Left(e)              =>
               log.warn(s"Error updating checkpoint: $e") *> ZIO.fail(Left(e))
           }.onSuccess { _ =>
             (updateStateWithDiagnosticEvents(_.updateLease(updatedLease, _)) *>
               (
                 leaseCompleted.succeed(()) *>
                   updateStateWithDiagnosticEvents(_.releaseLease(updatedLease, _)) *>
                   emitDiagnostic(DiagnosticEvent.LeaseReleased(shard)) *>
                   emitDiagnostic(DiagnosticEvent.ShardEnded(shard)).when(shardEnded) <*
                   takeLeases.fork.when(shardEnded)
               ).when(release)).orDie
           }
    } yield ()

  def releaseLeases: ZIO[Logging with Clock, Nothing, Unit] =
    log.debug("Starting releaseLeases") *>
      state.get
        .map(_.heldLeases.values)
        .flatMap(ZIO.foreachParN_(settings.maxParallelLeaseRenewals)(_) {
          case (lease, _) =>
            serialExecutionByShard(lease.key)(releaseLease(lease.key)).ignore // We do our best to release the lease
        }) *> log.debug("releaseLeases done")
}

private[zionative] object DefaultLeaseCoordinator {
  def make(
    applicationName: String,
    workerId: String,
    emitDiagnostic: DiagnosticEvent => UIO[Unit] = _ => UIO.unit,
    settings: LeaseCoordinationSettings,
    shards: Task[Map[String, Shard]],
    strategy: ShardAssignmentStrategy,
    initialPosition: InitialPosition
  ): ZManaged[
    Clock with Random with Logging with LeaseRepository,
    Throwable,
    LeaseCoordinator
  ] =
    (for {
      acquiredLeases  <- Queue
                          .bounded[(Lease, Promise[Nothing, Unit])](128)
                          .toManaged(_.shutdown)
                          .ensuring(log.debug("Acquired leases queue shutdown"))
      table           <- ZIO.service[LeaseRepository.Service].toManaged_
      state           <- Ref.make(State.empty).toManaged_
      serialExecution <- SerialExecution.keyed[String]().ensuringFirst(log.debug("Shutting down runloop"))
      c                = new DefaultLeaseCoordinator(
            table,
            applicationName,
            workerId,
            state,
            acquiredLeases,
            emitDiagnostic,
            serialExecution,
            settings,
            strategy,
            initialPosition
          )
      _               <- ZManaged.finalizer(
             c.releaseLeases.tap(_ => log.debug("releaseLeases done"))
           ) // We need the runloop to be alive for this operation

      // Wait for shards if the lease table does not exist yet, otherwise we assume there's leases
      // for all shards already, so just fork it. If there's new shards, the next `takeLeases` will
      // claim leases for them.
      // Optimized for the 'second startup or later' situation: lease table exists and covers all shards
      // Consumer will periodically update shards anyway
      _               <- (for {
               _ <- c.refreshLeases.catchSome {
                      case _: ResourceNotFoundException =>
                        ZIO
                          .service[LeaseRepository.Service]
                          .flatMap(_.createLeaseTableIfNotExists(applicationName)) *>
                          (shards >>= c.updateShards)
                    }.toManaged_
               // Initialization. If it fails, we will try in the loop
               _ <- (c.takeLeases.ignore *>
                        // Periodic refresh
                        repeatAndRetry(settings.refreshAndTakeInterval) {
                          (c.refreshLeases *> c.takeLeases)
                            .tapCause(e => log.error("Refresh & take leases failed, will retry", e))
                        }).forkManaged.ensuringFirst(log.debug("Shutting down refresh & take lease loop"))
               _ <- repeatAndRetry(settings.renewInterval) {
                      c.renewLeases
                        .tapCause(e => log.error("Renewing leases failed, will retry", e))
                    }.forkManaged.ensuringFirst(log.debug("Shutting down renew lease loop"))
             } yield ()).fork

    } yield c)
      .tapCause(c => log.error("Error creating DefaultLeaseCoordinator", c).toManaged_)

  /**
   * A shard can be consumed when it itself has not been processed until the end yet (lease checkpoint = SHARD_END)
   * and all its parents have been processed to the end (lease checkpoint = SHARD_END) or the parent shards have
   * expired
   */
  def shardsReadyToConsume(
    shards: Map[String, Shard],
    leases: Map[String, Lease]
  ): Map[String, Shard] =
    shards.filter {
      case (shardId, s) =>
        val shardProcessedToEnd = !leases.get(shardId).exists(shardHasEnded)
        shardProcessedToEnd && (parentShardsCompleted(s, leases) || allParentShardsExpired(s, shards))
    }

  private def shardHasEnded(l: Lease) = l.checkpoint.contains(Left(SpecialCheckpoint.ShardEnd))

  private def parentShardsCompleted(shard: Shard, leases: Map[String, Lease]): Boolean =
    //    println(
    //      s"Shard ${shard} has parents ${shard.parentShardIds.mkString(",")}. " +
    //        s"Leases for these: ${shard.parentShardIds.map(id => leases.find(_.key == id).mkString(","))}"
    //    )

    // Either there is no lease / the lease has already been cleaned up or it is checkpointed with SHARD_END
    shard.parentShardIds.forall(leases.get(_).exists(shardHasEnded))

  def allParentShardsExpired(shard: Shard, shards: Map[String, Shard]): Boolean =
    shard.parentShardIds.flatMap(shards.get).isEmpty

  def initialCheckpointForShard(
    shard: Shard,
    initialPosition: InitialPosition,
    allLeases: Map[String, Lease]
  ): SpecialCheckpoint =
    initialPosition match {
      case InitialPosition.TrimHorizon                => SpecialCheckpoint.TrimHorizon
      case InitialPosition.AtTimestamp(timestamp @ _) => SpecialCheckpoint.AtTimestamp
      case InitialPosition.Latest                     =>
        if (!shard.hasParents) SpecialCheckpoint.Latest
        else {
          val parentLeases = shard.parentShardIds.flatMap(allLeases.get)
          if (parentLeases.isEmpty) SpecialCheckpoint.Latest
          else SpecialCheckpoint.TrimHorizon
        }
    }

  private def repeatAndRetry[R, E, A](interval: Duration)(effect: ZIO[R, E, A]) =
    effect.repeat(Schedule.fixed(interval)).delay(interval).retry(Schedule.forever)

  final case class LeaseState(lease: Lease, completed: Option[Promise[Nothing, Unit]], lastUpdated: Instant) {
    def update(updatedLease: Lease, now: Instant) =
      copy(lease = updatedLease, lastUpdated = if (updatedLease.counter > lease.counter) now else lastUpdated)

    def isExpired(now: Instant, expirationTime: Duration) =
      lastUpdated isBefore (now minusMillis expirationTime.toMillis)

    def wasUpdatedLessThan(interval: Duration, now: Instant) =
      lastUpdated isAfter (now minusMillis interval.toMillis)
  }

  /**
   *
   * @param currentLeases Latest known state of all leases
   * @param shards List of all of the stream's shards which are currently active
   */
  final case class State(
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

  implicit class ShardExtensions(s: Shard) {
    def parentShardIds: Seq[String] = Option(s.parentShardId()).toList ++ Option(s.adjacentParentShardId()).toList
    def hasParents: Boolean         = parentShardIds.nonEmpty
  }
}
