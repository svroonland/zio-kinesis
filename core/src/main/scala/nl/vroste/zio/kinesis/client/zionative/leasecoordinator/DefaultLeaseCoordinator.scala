package nl.vroste.zio.kinesis.client.zionative.leasecoordinator

import nl.vroste.zio.kinesis.client.Util.ZStreamExtensions
import nl.vroste.zio.kinesis.client.zionative.Consumer.InitialPosition
import nl.vroste.zio.kinesis.client.zionative.LeaseCoordinator.AcquiredLease
import nl.vroste.zio.kinesis.client.zionative.LeaseRepository.{
  Lease,
  LeaseAlreadyExists,
  LeaseObsolete,
  UnableToClaimLease
}
import nl.vroste.zio.kinesis.client.zionative._
import nl.vroste.zio.kinesis.client.zionative.leasecoordinator.DefaultLeaseCoordinator.State
import nl.vroste.zio.kinesis.client.zionative.leasecoordinator.ZioExtensions.foreachParNUninterrupted_
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
import zio._
import zio.aws.kinesis.model.Shard
import zio.aws.kinesis.model.primitives.ShardId
import zio.stream.ZStream

import java.time.{ DateTimeException, Instant }

/**
 * Coordinates leases for shards between different workers
 *
 * How it works:
 *
 *   - There's a lease table with a record for every shard.
 *   - The lease owner is the identifier of the worker that has currently claimed the lease. No owner means that the
 *     previous worked has released the lease. The lease is released (owner set to null) when the application shuts
 *     down.
 *   - The checkpoint is the sequence number of the last processed record
 *   - The lease counter is an atomically updated counter to prevent concurrent changes. If it's not what expected when
 *     updating, another worker has probably stolen the lease. It is updated for every change to the lease.
 *   - Leases are periodically refreshed for the purposes of:
 *     1. Detecting that another worker has stolen our lease if we checkpoint at larger intervals than the lease renewal
 *        interval. 2. Detecting the number of active workers as input for the lease stealing algorithm 3. Detecting
 *        zombie workers / workers that left, so we can take their leases
 *   - Parent shard IDs: when the shard was created by resharding, which shard(s) is its predecessor
 *
 * Terminology:
 *   - held lease: a lease held by this worker
 */
private class DefaultLeaseCoordinator(
  table: LeaseRepository,
  applicationName: String,
  workerId: String,
  state: Ref[State],
  acquiredLeasesQueue: Queue[(Lease, Promise[Nothing, Unit])],
  emitDiagnostic: DiagnosticEvent => UIO[Unit] = _ => ZIO.unit,
  serialExecutionByShard: SerialExecution[String],
  settings: LeaseCoordinationSettings,
  strategy: ShardAssignmentStrategy,
  initialPosition: InitialPosition,
  shards: Task[Map[ShardId, Shard.ReadOnly]]
) extends LeaseCoordinator {

  import DefaultLeaseCoordinator._
  import ZioExtensions.OnSuccessSyntax

  val now = zio.Clock.currentDateTime.map(_.toInstant())

  private def initialize: ZIO[Scope, Throwable, Unit] = {
    val getLeasesOrInitializeLeaseTable = refreshLeases.catchSome { case _: ResourceNotFoundException =>
      table.createLeaseTableIfNotExists(applicationName) *> shards.flatMap(updateShards)
    }

    val periodicRefreshAndTakeLeases = repeatAndRetry(settings.refreshAndTakeInterval) {
      (refreshLeases *> takeLeases)
        .tapError(e => ZIO.logError(s"Refresh & take leases failed, will retry: ${e}"))
    }

    // Optimized for the 'second startup or later' situation: lease table exists and covers all shards
    // Consumer will periodically update shards anyway
    // Wait for shards if the lease table does not exist yet, otherwise we assume there's leases
    // for all shards already, so just fork it. If there's new shards, the next `takeLeases` will
    // claim leases for them.
    (for {
      _ <- getLeasesOrInitializeLeaseTable
      // Initialization. If it fails, we will try in the loop
      _ <- (takeLeases.retryN(1).ignore *> periodicRefreshAndTakeLeases)
             .ensuring(ZIO.logDebug("Shutting down refresh & take lease loop"))
             .forkScoped
      _ <- repeatAndRetry(settings.renewInterval)(renewLeases)
             .ensuring(ZIO.logDebug("Shutting down renew lease loop"))
             .forkScoped
    } yield ())
      .tapErrorCause(c => ZIO.logErrorCause("Error in DefaultLeaseCoordinator initialize", c))
  }

  override def updateShards(shards: Map[ShardId, Shard.ReadOnly]): UIO[Unit] =
    updateStateWithDiagnosticEvents(_.updateShards(shards))

  override def childShardsDetected(
    childShards: Seq[Shard.ReadOnly]
  ): ZIO[Any, Throwable, Unit] =
    updateStateWithDiagnosticEvents(s =>
      s.updateShards(s.shards ++ childShards.map(shard => shard.shardId -> shard).toMap)
    )

  private def updateStateWithDiagnosticEvents(f: (State, Instant) => State): ZIO[Any, DateTimeException, Unit] =
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
      _                        <- ZIO.foreachDiscard(newShards)(shardId => emitDiagnostic(DiagnosticEvent.NewShardDetected(shardId)))
      _                        <- ZIO.foreachDiscard(removedShards)(shardId => emitDiagnostic(DiagnosticEvent.ShardEnded(shardId)))
    } yield ()

  // Emits WorkerJoined and WorkerLeft after each lease operation
  private def emitWorkerPoolDiagnostics(currentLeases: Iterable[Lease], newLeases: Iterable[Lease]): UIO[Unit] = {
    val currentWorkers = currentLeases.map(_.owner).collect { case Some(owner) => owner }.toSet
    val newWorkers     = newLeases.map(_.owner).collect { case Some(owner) => owner }.toSet
    val workersJoined  = newWorkers -- currentWorkers
    val workersLeft    = currentWorkers -- newWorkers

    for {
      _ <- ZIO.foreachDiscard(workersJoined)(w => emitDiagnostic(DiagnosticEvent.WorkerJoined(w)))
      _ <- ZIO.foreachDiscard(workersLeft)(w => emitDiagnostic(DiagnosticEvent.WorkerLeft(w)))
    } yield ()
  }

  def leaseLost(lease: Lease, leaseCompleted: Promise[Nothing, Unit]): ZIO[Any, Throwable, Unit] =
    leaseCompleted.succeed(()) *>
      updateStateWithDiagnosticEvents((s, now) => s.releaseLease(lease, now).updateLease(lease.release, now)) *>
      emitDiagnostic(DiagnosticEvent.ShardLeaseLost(lease.key))

  def releaseLease(shard: String) =
    state.get.flatMap(
      _.getHeldLease(shard)
        .map(releaseHeldLease(_))
        .getOrElse(ZIO.unit)
    )

  def releaseHeldLease: ((Lease, Promise[Nothing, Unit])) => ZIO[Any, Throwable, Unit] = { case (lease, completed) =>
    val updatedLease = lease.copy(owner = None).increaseCounter

    table
      .releaseLease(applicationName, updatedLease)
      // TODO THIS TIMEOUT CAUSES THE INTERRUPTION ERROR...! So it's about racing in the finalizer..
//      .timeout(settings.releaseLeaseTimeout)
      .asSome
      .tap(result =>
        ZIO.logWarning(s"Timeout while releasing lease for shard ${lease.key}, ignored").when(result.isEmpty)
      )
      .tapError {
        case Right(LeaseObsolete) => // This is fine at shutdown
          ZIO.unit
        case Left(e)              =>
          ZIO.logError(s"Error releasing lease for shard ${lease.key}, ignored: ${e}")
      }
      .ignore *>
      completed.succeed(()) *>
      updateStateWithDiagnosticEvents(_.releaseLease(updatedLease, _)) *>
      emitDiagnostic(DiagnosticEvent.LeaseReleased(lease.key))
  }

  /**
   * Increases the lease counter of held leases. Other workers can observe this and know that this worker is still
   * active.
   *
   * Leases that recently had their checkpoint updated are not updated here to save on DynamoDB usage
   */
  val renewLeases: ZIO[Any, Throwable, Unit] = (for {
    currentLeases <- state.get.map(_.currentLeases)
    now           <- now
    leasesToRenew  = currentLeases.view.collect {
                       case (shard, leaseState)
                           if !leaseState.wasUpdatedLessThan(settings.renewInterval, now) && leaseState.lease.owner
                             .contains(workerId) =>
                         shard
                     }
    _             <- ZIO.logDebug(s"Renewing ${leasesToRenew.size} leases")
    _             <- foreachParNUninterrupted_(settings.maxParallelLeaseRenewals)(leasesToRenew) { shardId =>
                       serialExecutionByShard(shardId)(renewLease(shardId))
                         .tapError(e => ZIO.logError(s"Error renewing lease: ${e}"))
                         .retry(settings.renewRetrySchedule) orElse (
                         ZIO.logWarning(s"Failed to renew lease for shard ${shardId}, releasing") *>
                           serialExecutionByShard(shardId)(releaseLease(shardId))
                       )
                     }
    // _             <- ZIO.logDebug(s"Renewing ${leasesToRenew.size} leases done")
  } yield ())
    .tapError(e => ZIO.logError(s"Renewing leases failed, will retry: ${e}"))

  // Lease renewal increases the counter only. May detect that lease was stolen
  private def renewLease(shard: String): ZIO[Any, Throwable, Unit] =
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
              ZIO.logInfo(s"Unable to renew lease for shard, lease counter was obsolete")
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
  val refreshLeases: ZIO[Any, Throwable, Unit] =
    for {
      _        <- ZIO.logInfo("Refreshing leases")
      duration <- table
                    .getLeases(applicationName)
                    .mapChunksZIO(
                      ZIO
                        .foreachParDiscard(_) { lease =>
                          ZIO.logInfo(s"RefreshLeases: ${lease}") *>
                            serialExecutionByShard(lease.key)(refreshLease(lease))
                        }
                        .as(Chunk.unit)
                    )
                    .runDrain
                    .timed
                    .map(_._1)
      _        <- ZIO.logDebug(s"Refreshing leases took ${duration.toMillis}")
    } yield ()

  private def refreshLease(lease: Lease): ZIO[Any, Throwable, Unit] =
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

  private def registerNewAcquiredLease(lease: Lease): ZIO[Any, Nothing, Unit] =
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
    shards: Map[String, Shard.ReadOnly],
    leases: Map[String, Lease]
  ): ZIO[Any, Throwable, Unit] =
    for {
      _                 <- ZIO.logInfo(s"Found ${leases.size} leases")
      shardsWithoutLease = desiredShards.filterNot(leases.contains).toSeq.sorted.map(shards(_))
      _                 <- ZIO
                             .logInfo(
                               s"No leases exist yet for these shards, creating and claiming: " +
                                 s"${shardsWithoutLease.map(_.shardId).mkString(",")}"
                             )
                             .when(shardsWithoutLease.nonEmpty)
      _                 <- ZIO
                             .foreachParDiscard(shardsWithoutLease)({ shard =>
                               val lease = Lease(
                                 key = shard.shardId,
                                 owner = Some(workerId),
                                 counter = 0L,
                                 checkpoint = Some(Left(initialCheckpointForShard(shard, initialPosition, leases))),
                                 parentShardIds = shard.parentShardIds
                               )

                               (table.createLease(applicationName, lease) <* serialExecutionByShard(lease.key)(
                                 registerNewAcquiredLease(lease)
                               )).catchAll {
                                 case Right(LeaseAlreadyExists) =>
                                   ZIO.logInfo(s"Unable to claim lease for shard ${lease.key}, beaten to it by another worker?")
                                 case Left(e)                   =>
                                   ZIO.logError(s"Error creating lease: ${e}") *> ZIO.fail(e)
                               }
                             })
                             .withParallelism(settings.maxParallelLeaseAcquisitions)
    } yield ()

  /**
   * Takes leases, unowned or from other workers, to get this worker's number of leases to the target value (nr leases /
   * nr workers)
   *
   * Taking individual leases may fail, but the others will still be tried. Only in the next round of lease taking will
   * we try again.
   *
   * The effect fails with a Throwable when having failed to take one or more leases
   */
  val takeLeases: ZIO[Any, Throwable, Unit] =
    for {
      leases          <- state.get.map(_.currentLeases.values.toSet)
      shards          <- state.get.map(_.shards.view.map { case (k, v) => ShardId.unwrap(k) -> v }.toMap)
      leaseMap         = leases.map(s => s.lease.key -> s.lease).toMap
      openLeases       = leases.collect {
                           case LeaseState(lease, completed @ _, lastUpdated) if !shardHasEnded(lease) =>
                             (lease, lastUpdated)
                         }
      consumableShards = shardsReadyToConsume(shards, leaseMap)
      desiredShards   <- strategy.desiredShards(openLeases, consumableShards.keySet, workerId)
      _               <- ZIO.logInfo(s"Desired shard assignment: ${desiredShards.mkString(",")}")
      toTake           = leaseMap.values.filter(l => desiredShards.contains(l.key) && !l.owner.contains(workerId))
      _               <- claimLeasesForShardsWithoutLease(desiredShards, shards, leaseMap)
      _               <- ZIO
                           .logInfo(s"Going to take ${toTake.size} leases from other workers: ${toTake.mkString(",")}")
                           .when(toTake.nonEmpty)
      _               <- foreachParNUninterrupted_(settings.maxParallelLeaseAcquisitions)(toTake) { lease =>
                           val updatedLease = lease.claim(workerId)
                           (table.claimLease(applicationName, updatedLease) *>
                             serialExecutionByShard(updatedLease.key)(registerNewAcquiredLease(updatedLease))).catchAll {
                             case Right(UnableToClaimLease) =>
                               ZIO.logInfo(
                                 s"Unable to claim lease for shard ${lease.key}, beaten to it by another worker?"
                               )
                             case Left(e)                   =>
                               ZIO.logError(s"Got error ${e}") *> ZIO.fail(e)
                           }
                         }
    } yield ()

  override def acquiredLeases: ZStream[Any, Throwable, AcquiredLease] = ZStream.unwrapScoped {
    for {
      // We need a forked scope with independent finalizer order, because `initialize` will call `forkScoped` after being forked,
      // which leads to a wrong finalizer order
      scope        <- ZIO.scope
      childScope   <- scope.fork
      runloopFiber <- initialize
                        .provideEnvironment(ZEnvironment(childScope))
                        .forkScoped
      _            <- ZIO.addFinalizer(
                        releaseLeases *> ZIO.logDebug("releaseLeases done")
                      ) // We need the runloop to be alive for this operation
    } yield ZStream
      .fromQueue(acquiredLeasesQueue)
      .map { case (lease, complete) => AcquiredLease(lease.key, complete) }
      .terminateOnFiberFailure(runloopFiber)
  }

  override def getCheckpointForShard(shardId: String): UIO[Option[Either[SpecialCheckpoint, ExtendedSequenceNumber]]] =
    for {
      leaseOpt <- state.get.map(_.currentLeases.get(shardId))
    } yield leaseOpt.flatMap(_.lease.checkpoint)

  override def makeCheckpointer(shardId: String) =
    for {
      state  <- Ref.make(DefaultCheckpointer.State.empty)
      permit <- Semaphore.make(1)
    } yield new DefaultCheckpointer(
      shardId,
      state,
      permit,
      (checkpoint, release) => serialExecutionByShard(shardId)(updateCheckpoint(shardId, checkpoint, release)),
      serialExecutionByShard(shardId)(releaseLease(shardId))
    )

  private def updateCheckpoint(
    shard: String,
    checkpoint: Either[SpecialCheckpoint, ExtendedSequenceNumber],
    release: Boolean
  ): ZIO[Any, Either[Throwable, ShardLeaseLost.type], Unit] =
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
                                    ZIO.logWarning(s"Error updating checkpoint: $e") *> ZIO.fail(Left(e))
                                }.onSuccess { _ =>
                                  (updateStateWithDiagnosticEvents(_.updateLease(updatedLease, _)) *>
                                    (
                                      leaseCompleted.succeed(()) *>
                                        updateStateWithDiagnosticEvents(_.releaseLease(updatedLease, _)) *>
                                        emitDiagnostic(DiagnosticEvent.LeaseReleased(shard)) *>
                                        emitDiagnostic(DiagnosticEvent.ShardEnded(shard)).when(shardEnded) <*
                                        takeLeases.ignore.fork.when(shardEnded) // When it fails, the runloop will try it again sooner
                                    ).when(release)).orDie
                                }
    } yield ()

  def releaseLeases: ZIO[Any, Nothing, Unit] =
    ZIO.logDebug("Starting releaseLeases") *>
      state.get
        .map(_.heldLeases.values)
        .flatMap(
          ZIO
            .foreachParDiscard(_) { case (lease, _) =>
              serialExecutionByShard(lease.key)(releaseLease(lease.key)).ignore // We do our best to release the lease
            }
            .withParallelism(settings.maxParallelLeaseRenewals)
        ) *> ZIO.logDebug("releaseLeases done")
}

private[zionative] object DefaultLeaseCoordinator {
  def make(
    applicationName: String,
    workerId: String,
    emitDiagnostic: DiagnosticEvent => UIO[Unit] = _ => ZIO.unit,
    settings: LeaseCoordinationSettings,
    shards: Task[Map[ShardId, Shard.ReadOnly]],
    strategy: ShardAssignmentStrategy,
    initialPosition: InitialPosition
  ): ZIO[Scope with LeaseRepository, Throwable, LeaseCoordinator] =
    (for {
      acquiredLeases  <- ZIO
                           .acquireRelease(
                             Queue
                               .bounded[(Lease, Promise[Nothing, Unit])](128)
                           )(_.shutdown)
                           .ensuring(ZIO.logDebug("Acquired leases queue shutdown"))
      table           <- ZIO.service[LeaseRepository]
      state           <- Ref.make(State.empty)
      serialExecution <- SerialExecution.keyed[String].ensuring(ZIO.logDebug("Shutting down runloop"))
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
                           initialPosition,
                           shards
                         )
    } yield c)
      .tapErrorCause(c => ZIO.logSpan("Error creating DefaultLeaseCoordinator")(ZIO.logErrorCause(c)))

  /**
   * A shard can be consumed when it itself has not been processed until the end yet (lease checkpoint = SHARD_END) and
   * all its parents have been processed to the end (lease checkpoint = SHARD_END) or the parent shards have expired
   */
  def shardsReadyToConsume(
    shards: Map[String, Shard.ReadOnly],
    leases: Map[String, Lease]
  ): Map[String, Shard.ReadOnly] =
    shards.filter { case (shardId, s) =>
      val shardProcessedToEnd = !leases.get(shardId).exists(shardHasEnded)
      shardProcessedToEnd && (parentShardsCompleted(s, leases) || allParentShardsExpired(s, shards.keySet))
    }

  private def shardHasEnded(l: Lease) = l.checkpoint.contains(Left(SpecialCheckpoint.ShardEnd))

  private def parentShardsCompleted(shard: Shard.ReadOnly, leases: Map[String, Lease]): Boolean =
    //    println(
    //      s"Shard ${shard} has parents ${shard.parentShardIds.mkString(",")}. " +
    //        s"Leases for these: ${shard.parentShardIds.map(id => leases.find(_.key == id).mkString(","))}"
    //    )

    // Either there is no lease / the lease has already been cleaned up or it is checkpointed with SHARD_END
    shard.parentShardIds.forall(leases.get(_).exists(shardHasEnded))

  def allParentShardsExpired(shard: Shard.ReadOnly, shards: Set[String]): Boolean =
    !shard.parentShardIds.exists(shards.apply)

  def initialCheckpointForShard(
    shard: Shard.ReadOnly,
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

  private def repeatAndRetry[R, E, A](
    interval: Duration
  )(effect: ZIO[R, E, A]): ZIO[R, Nothing, Unit] =
    ZIO.interruptibleMask { restore =>
      restore(effect)
        .repeat(Schedule.fixed(interval))
        .delay(interval)
        .retry(Schedule.forever)
        .ignore // Cannot fail
    }

  final case class LeaseState(lease: Lease, completed: Option[Promise[Nothing, Unit]], lastUpdated: Instant) {
    def update(updatedLease: Lease, now: Instant) =
      copy(lease = updatedLease, lastUpdated = if (updatedLease.counter > lease.counter) now else lastUpdated)

    def isExpired(now: Instant, expirationTime: Duration) =
      lastUpdated isBefore (now minusMillis expirationTime.toMillis)

    def wasUpdatedLessThan(interval: Duration, now: Instant) =
      lastUpdated isAfter (now minusMillis interval.toMillis)
  }

  /**
   * @param currentLeases
   *   Latest known state of all leases
   * @param shards
   *   List of all of the stream's shards which are currently active
   */
  final case class State(
    currentLeases: Map[String, LeaseState],
    shards: Map[ShardId, Shard.ReadOnly]
  ) {
    val heldLeases = currentLeases.collect { case (shard, LeaseState(lease, Some(completed), _)) =>
      shard -> (lease -> completed)
    }

    def updateShards(shards: Map[ShardId, Shard.ReadOnly]): State = copy(shards = shards)

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

    def make(leases: List[Lease], shards: Map[ShardId, Shard.ReadOnly]): ZIO[Any, Nothing, State] =
      Clock.currentDateTime.map(_.toInstant()).map { now =>
        State(leases.map(l => l.key -> LeaseState(l, None, now)).toMap, shards)
      }
  }

  implicit class ShardExtensions(s: Shard.ReadOnly) {
    def parentShardIds: Seq[String] = s.parentShardId.toList ++ s.adjacentParentShardId.toList
    def hasParents: Boolean         = parentShardIds.nonEmpty
  }
}
