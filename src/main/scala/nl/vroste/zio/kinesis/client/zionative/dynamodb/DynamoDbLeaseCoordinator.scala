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
import zio.random.Random
import zio.stream.ZStream
import scala.util.control.NoStackTrace
import nl.vroste.zio.kinesis.client.zionative.dynamodb.DynamoDbLeaseCoordinator.LeaseCommand.RefreshLease

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
    // require(
    //   currentLeases.get(lease.key).forall(_.counter == lease.counter - 1),
    //   s"Unexpected lease counter while updating lease ${lease}"
    // )
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
  import zio.random.shuffle

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
      done: Promise[Either[Throwable, ShardLeaseLost.type], Unit]
    ) =
      (for {
        heldleaseWithComplete  <- state.get
                                   .map(_.heldLeases.get(shard))
                                   .someOrFail(Right(ShardLeaseLost): Either[Throwable, ShardLeaseLost.type])
        (lease, leaseCompleted) = heldleaseWithComplete
        updatedLease            = lease.copy(counter = lease.counter + 1, checkpoint = Some(checkpoint))
        _                      <- (table
                 .updateCheckpoint(updatedLease) <* emitDiagnostic(
                 DiagnosticEvent.Checkpoint(shard, checkpoint)
               )).catchAll {
               case _: ConditionalCheckFailedException =>
                 leaseLost(updatedLease, leaseCompleted) *>
                   ZIO.fail(Right(ShardLeaseLost))
               case e                                  =>
                 ZIO.fail(Left(e))
             }.onSuccess(_ => state.update(_.updateLease(updatedLease)))
      } yield ()).foldM(done.fail, done.succeed)

    // Lease renewal increases the counter only. May detect that lease was stolen
    def doRenewLease(shard: String, done: Promise[Throwable, Unit]) =
      state.get.map(_.getHeldLease(shard)).map {
        case Some((lease, leaseCompleted)) =>
          val updatedLease = lease.increaseCounter
          (for {
            _ <- table.renewLease(updatedLease).catchAll {
                   // This means the lease was updated by another worker
                   case Right(LeaseObsolete) =>
                     leaseLost(lease, leaseCompleted) *>
                       log.info(s"Unable to renew lease for shard, lease counter was obsolete").unit
                   case Left(e)              => ZIO.fail(e)
                 }
            _ <- state.update(_.updateLease(updatedLease))
            _ <- emitDiagnostic(DiagnosticEvent.LeaseRenewed(updatedLease.key))
          } yield ()).foldM(done.fail, done.succeed)
        case None                          =>
          done.fail(new Exception(s"Unknown lease for shard ${shard}! This indicates a programming error"))
      }

    def doRefreshLease(lease: Lease, done: Promise[Nothing, Unit]): UIO[Unit] =
      for {
        currentState <- state.get
        shardId       = lease.key
        _            <- (currentState.getLease(shardId), currentState.getHeldLease(shardId)) match {
               // This is one of our held leases, we expect it to be unchanged
               case (Some(previousLease), Some(_)) if previousLease.counter == lease.counter                   =>
                 ZIO.unit
               // This is our held lease that was stolen by another worker
               case (Some(previousLease), Some((_, leaseCompleted))) if previousLease.counter != lease.counter =>
                 leaseLost(lease, leaseCompleted)
               // We found a new lease
               case (None, _)                                                                                  =>
                 state.update(_.updateLease(lease))
             }
      } yield ()

    ZStream
      .fromQueue(commandQueue)
      .groupByKey(_.shard) {
        case (shard @ _, command) =>
          command.mapM {
            case UpdateCheckpoint(shard, checkpoint, done) =>
              doUpdateCheckpoint(shard, checkpoint, done)
            case RenewLease(shard, done)                   =>
              doRenewLease(shard, done)
            case RefreshLease(lease, done)                 =>
              doRefreshLease(lease, done)
          }
      }
  }

  def leasesToSteal(state: State) = {

    val allLeases      = state.currentLeases.values.toList
    val shards         = state.shards
//    val leasesByWorker = allLeases.groupBy(_.owner)
    val ourLeases      = state.heldLeases.values.map(_._1).toList
    val allWorkers     = allLeases.map(_.owner).collect { case Some(owner) => owner }.toSet ++ Set(workerId)
    val nrLeasesToHave = Math.floor(shards.size * 1.0 / (allWorkers.size * 1.0)).toInt
    println(
      s"We have ${ourLeases.size}, we would like to have ${nrLeasesToHave}/${allLeases.size} leases (${allWorkers.size} workers)"
    )
    if (ourLeases.size < nrLeasesToHave) {
      val nrLeasesToSteal = nrLeasesToHave - ourLeases.size
      // Steal leases
      for {
        otherLeases  <- shuffle(allLeases.filterNot(_.owner.contains(workerId)))
        leasesToSteal = otherLeases.take(nrLeasesToSteal)
        _            <- log.info(s"Going to steal ${nrLeasesToSteal} leases: ${leasesToSteal.mkString(",")}")
      } yield leasesToSteal
    } else ZIO.succeed(List.empty)
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
      _      <- ZIO.foreach_(leases)(lease => processCommand(LeaseCommand.RefreshLease(lease, _)))
    } yield ()
  }

  /**
   * A single round of lease stealing
   *
   * - First take expired leases (TODO)
   * - Then take X leases from the most loaded worked
   */
  val stealLeases =
    for {
      _             <- log.info("Stealing leases")
      state         <- state.get
      toSteal       <- leasesToSteal(state)
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
      _         <- acquiredLeasesQueue.offer(lease -> completed)
      _         <- emitDiagnostic(DiagnosticEvent.LeaseAcquired(lease.key, lease.checkpoint))
    } yield ()

  /**
   * Claims all available leases for the current shards (no owner or not existent)
   */
  def initializeLeases: ZIO[Logging, Throwable, Unit] =
    for {
      state                       <- state.get
      allLeases                    = state.currentLeases
      // Claim new leases for the shards the database doesn't have leases for
      shardsWithoutLease           =
        state.shards.values.toList.filterNot(shard => allLeases.values.map(_.key).toList.contains(shard.shardId()))
      _                           <- log.info(
             s"No leases exist yet for these shards, creating: ${shardsWithoutLease.map(_.shardId()).mkString(",")}"
           )
      leasesForShardsWithoutLease <- ZIO
                                       .foreach(shardsWithoutLease) { shard =>
                                         val lease = Lease(
                                           key = shard.shardId(),
                                           owner = Some(workerId),
                                           counter = 0L,
                                           ownerSwitchesSinceCheckpoint = 0L,
                                           checkpoint = None,
                                           parentShardIds = Seq.empty
                                         )

                                         table.createLease(lease).as(Some(lease)).catchAll {
                                           case Right(LeaseAlreadyExists) =>
                                             log
                                               .info(
                                                 s"Unable to claim lease for shard ${lease.key}, beaten to it by another worker?"
                                               )
                                               .as(None)
                                           case Left(e)                   =>
                                             ZIO.fail(e)
                                         }
                                       }
                                       .map(_.flatten)
      // Claim leases without owner
      // TODO we can also claim leases that have expired (worker crashed before releasing)
      claimableLeases              = allLeases.filter { case (key @ _, lease) => lease.owner.isEmpty }
      _                           <- log.info(s"Claim of leases without owner for shards ${claimableLeases.mkString(",")}")
      // TODO this might be somewhat redundant with lease stealing
      claimedLeases               <- ZIO
                         .foreachPar(claimableLeases) {
                           case (key @ _, lease) =>
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
                                   ZIO.fail(e)
                               }
                         }
                         .map(_.flatten)
      _                           <- ZIO.foreach_(leasesForShardsWithoutLease ++ claimedLeases)(registerNewAcquiredLease)
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
//      clock  <- ZIO.environment[Clock with Logging]
      // logging <- ZIO.environment[Logging]
    } yield new Checkpointer {
      override def checkpoint: ZIO[zio.blocking.Blocking, Either[Throwable, ShardLeaseLost.type], Unit] =
        /**
         * This uses a semaphore to ensure that two concurrent calls to `checkpoint` do not attempt
         * to process the
         *
         * It is fine to stage new records for checkpointing though, those will be processed in the
         * next call tho `checkpoint`
         *
         * If a new record is staged while the checkpoint operation is busy,
         *
         * TODO make a test for that: staging while checkpointing should not lose the staged checkpoint.
         */
        permit.withPermit {
          for {
            lastStaged <- staged.get
            _          <- lastStaged match {
                   case Some(checkpoint) =>
                     processCommand(LeaseCommand.UpdateCheckpoint(shard.shardId(), checkpoint, _))
                     // onSuccess to ensure updating in the face of interruption
                     // only update when the staged record has not changed while checkpointing
                       .onSuccess(_ => staged.updateSome { case Some(lastStaged) => None })
                   case None             => UIO.unit
                 }
          } yield ()
        }

      override def stage(r: Record[_]): zio.UIO[Unit] =
        staged.set(Some(ExtendedSequenceNumber(r.sequenceNumber, r.subSequenceNumber)))
    }

  def releaseLeases: ZIO[Logging, Throwable, Unit] =
    state.get
      .map(_.heldLeases.values)
      .flatMap(ZIO.foreachPar_(_)(releaseLease))

  override def releaseLease(shard: String) =
    state.get.flatMap(
      _.getHeldLease(shard)
        .map(releaseLease(_) *> emitDiagnostic(DiagnosticEvent.LeaseReleased(shard)))
        .getOrElse(ZIO.unit)
    )

  private def releaseLease: ((Lease, Promise[Nothing, Unit])) => ZIO[Logging, Throwable, Unit] = {
    case (lease, completed) =>
      val updatedLease = lease.copy(owner = None).increaseCounter

      table.releaseLease(updatedLease).catchAll {
        case Right(ShardLeaseLost) => // This is fine at shutdown
          ZIO.unit
        case Left(e)               =>
          ZIO.fail(e)
      } *> completed.succeed(()) *> state.update(_.updateLease(updatedLease).releaseLease(updatedLease))
  }
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
      done: Promise[Either[Throwable, ShardLeaseLost.type], Unit]
    ) extends LeaseCommand

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
          _              <- table.createLeaseTableIfNotExists
          currentLeases  <- table.getLeasesFromDB
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
    }(_.releaseLeases.orDie)
      .tap(_.runloop.runDrain.forkManaged)
      .tap(c => (c.initializeLeases *> c.stealLeases).toManaged_)
      .tap(c =>
        log
          .locally(LogAnnotation.Name(s"worker-${workerId}" :: Nil))(c.refreshLeases *> c.renewLeases *> c.stealLeases)
          .repeat(Schedule.fixed(5.seconds))
          .delay(5.seconds)
          .forkManaged
      )

}
