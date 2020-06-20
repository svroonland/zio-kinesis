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
 * - There's a lease table with an item for every shard.
 * - The lease owner is the worker identifier (?). No owner means that the previous worked has
 *   released the lease. The lease should be released when the application shuts down.
 * - The checkpoint is the checkpoint
 * - The lease counter is an atomically updated counter to prevent concurrent changes. If it's not
 *   what expected when updating, another worker has probably stolen the lease.
 * - Owner switches since checkpoint? Not sure what useful for.. TODO
 * - Parent shard IDs: not yet handled TODO
 * - pending checkpoint: not yet supported TODO
 *
 * TODO for now only single worker model checkpointing is supported
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

  val runloop = ZStream
    .fromQueue(commandQueue)
    .groupByKey(_.shard) {
      case (shard @ _, x) =>
        x.mapM {
          case UpdateCheckpoint(shard, checkpoint, done) =>
            (for {
              heldleaseWithComplete <- state.get
                                         .map(_.heldLeases.get(shard))
                                         .someOrFail(Right(ShardLeaseLost): Either[Throwable, ShardLeaseLost.type])
              (lease, _)             = heldleaseWithComplete
              updatedLease           = lease.copy(counter = lease.counter + 1, checkpoint = Some(checkpoint))
              _                     <- (table
                       .updateCheckpoint(updatedLease) <* emitDiagnostic(
                       DiagnosticEvent.Checkpoint(shard, checkpoint)
                     )).catchAll {
                     case _: ConditionalCheckFailedException =>
                       //  leaseLost(lease.key) *> refreshLeases
                       //    .mapError(Left(_))
                       //    .fork *>
                       ZIO.fail(Right(ShardLeaseLost))
                     case e                                  =>
                       ZIO.fail(Left(e))
                   }.onSuccess(_ => state.update(_.updateLease(updatedLease)))
            } yield ()).foldM(done.fail, done.succeed)

          case RenewLease(shard, done)                   =>
            state.get.map(_.getLease(shard)).map {
              case Some(lease) =>
                val updatedLease = lease.increaseCounter
                (for {
                  _ <- table.renewLease(updatedLease)
                  // Checkpointer may have already updated our lease
                  _ <- state.updateSome {
                         case s
                             if s.currentLeases
                               .get(updatedLease.key)
                               .exists(_.counter == updatedLease.counter - 1) =>
                           s.updateLease(updatedLease)
                       }
                  _ <- emitDiagnostic(DiagnosticEvent.LeaseRenewed(updatedLease.key))
                } yield Some(updatedLease)).catchAll {
                  // Either it was stolen or our checkpointer updated it as well (also updating the lease counter)
                  case Right(LeaseObsolete) =>
                    log.info(s"Unable to renew lease for shard, lease counter was obsolete").unit
                  case Left(e)              => ZIO.fail(e)
                }.unit.foldM(done.fail, done.succeed)
              case None        =>
                ZIO.unit
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
    heldLeases <- state.get.map(_.heldLeases.keySet)
    _          <- ZIO
           .foreach_(heldLeases)(shard => processCommand(LeaseCommand.RenewLease(shard, _)))
  } yield ()

  /**
   * For lease taking to work properly, we need to have the latest state of leases
   *
   * Also if other workers have taken our leases and we haven't yet checkpointed, we can find out proactively
   */
  val refreshLeases = {
    for {
      _                          <- log.info("Refreshing leases")
      leases                     <- table.getLeasesFromDB
      previousHeldLeases         <- state.getAndUpdate(_.updateLeases(leases)).map(_.heldLeases)
      leasesOwnedAccordingToDB    = leases.collect { case l if l.owner.contains(workerId) => l.key }.toSet
      leasesOwnedAccordingToState = previousHeldLeases.keySet
      lostLeases                  = leasesOwnedAccordingToState -- leasesOwnedAccordingToDB
      _                          <- log.info(s"${workerId} Leases in DB: ${leasesOwnedAccordingToDB.mkString(",")}")
      _                          <- log.info(s"${workerId} Leases in State: ${leasesOwnedAccordingToState.mkString(",")}")
      _                          <- log.info(s"${workerId} Lost leases ${lostLeases.mkString(",")}").when(lostLeases.nonEmpty)
      _                          <- ZIO.foreach_(lostLeases)(leaseLost)
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
      staged <- Ref.make[Option[Record[_]]](None)
//      clock  <- ZIO.environment[Clock with Logging]
      // logging <- ZIO.environment[Logging]
    } yield new Checkpointer {
      override def checkpoint: ZIO[zio.blocking.Blocking, Either[Throwable, ShardLeaseLost.type], Unit] =
        (for {
          lastStaged <- staged.get
          _          <- lastStaged match {
                 case Some(r) =>
                   val checkpoint = ExtendedSequenceNumber(r.sequenceNumber, r.subSequenceNumber)

                   processCommand(LeaseCommand.UpdateCheckpoint(shard.shardId(), checkpoint, _))
                     .onSuccess(_ => staged.set(None))
                 case None    => UIO.unit
               }
        } yield ())

      override def stage(r: Record[_]): zio.UIO[Unit] = staged.set(Some(r))
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

  private def leaseLost(shard: String): ZIO[Any, Nothing, Unit] =
    state
      .modify(
        s => // If there is a held lease, we remove it from the state and complete the promise, otherwise nothing is changed
          s.getHeldLease(shard)
            .map {
              case (lease, completed) =>
                (
                  completed.succeed(()) *> emitDiagnostic(DiagnosticEvent.ShardLeaseLost(shard)),
                  s.releaseLease(lease).updateLease(lease.copy(owner = None))
                )
            }
            .getOrElse((UIO.unit, s))
      )
      .flatten

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
    case class RefreshLease(shard: String, done: Promise[Nothing, Unit]) extends LeaseCommand
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
