package nl.vroste.zio.kinesis.client.zionative.dynamodb

import nl.vroste.zio.kinesis.client.DynamicConsumer.Record
import nl.vroste.zio.kinesis.client.Util.asZIO
import nl.vroste.zio.kinesis.client.zionative.LeaseCoordinator
import nl.vroste.zio.kinesis.client.zionative.Checkpointer
import nl.vroste.zio.kinesis.client.zionative.ShardLeaseLost
import nl.vroste.zio.kinesis.client.zionative.dynamodb.DynamoDbLeaseCoordinator.Lease
import nl.vroste.zio.kinesis.client.zionative.dynamodb.DynamoDbUtil._
import nl.vroste.zio.kinesis.client.Util.paginatedRequest
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._
import software.amazon.awssdk.services.kinesis.model.Shard
import zio._
import zio.clock.Clock
import zio.duration._

import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.Failure
import nl.vroste.zio.kinesis.client.zionative.dynamodb.DynamoDbLeaseCoordinator.LeaseAlreadyExists
import zio.stream.ZStream
import nl.vroste.zio.kinesis.client.zionative.LeaseCoordinator.AcquiredLease
import nl.vroste.zio.kinesis.client.zionative.dynamodb.DynamoDbLeaseCoordinator.UnableToClaimLease
import nl.vroste.zio.kinesis.client.zionative.dynamodb.DynamoDbLeaseCoordinator.ExtendedSequenceNumber
import zio.random.Random

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
    copy(
      currentLeases = currentLeases + (lease.key -> lease),
      heldLeases = heldLeases ++ (heldLeases.collect {
        case (shard, (_, completed)) if shard == lease.key => (shard, (lease, completed))
      })
    )

  def getLease(shardId: String): Option[Lease] =
    currentLeases.get(shardId)

  def getHeldLease(shardId: String): Option[(Lease, Promise[Nothing, Unit])] =
    heldLeases.get(shardId)

  def holdLease(lease: Lease, completed: Promise[Nothing, Unit]): State =
    copy(heldLeases = heldLeases + (lease.key -> (lease, completed)))

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
  applicationName: String,
  workerId: String,
  state: Ref[State],
  acquiredLeasesQueue: Queue[(Lease, Promise[Nothing, Unit])]
) extends LeaseCoordinator {

  import DynamoDbLeaseCoordinator._
  import ZioExtensions.OnSuccessSyntax
  import zio.random.shuffle

  /**
   *
   *
   * @param nr Number of leases to steal at once
   * @param period
   * @param leaseExpirationTime
   */
  case class LeaseCoordinationSettings(
    leasesToSteal: Int,
    period: Duration = 30.seconds,
    leaseExpirationTime: Duration = 1.minute
  )

  def leasesToSteal(state: State) = {

    val allLeases      = state.currentLeases.values.toList
    val shards         = state.shards
    val leasesByWorker = allLeases.groupBy(_.owner)
    val ourLeases      = state.heldLeases.values.map(_._1).toList
    val allWorkers     = allLeases.map(_.owner).collect { case Some(owner) => owner }.toSet ++ Set(workerId)
    val nrLeasesToHave = Math.floor(shards.size * 1.0 / (allWorkers.size * 1.0)).toInt
    println(
      s"Worker $workerId has ${ourLeases.size}, we would like to have ${nrLeasesToHave}/${allLeases.size} leases (${allWorkers.size} workers)"
    )
    if (ourLeases.size < nrLeasesToHave) {
      val nrLeasesToSteal = nrLeasesToHave - ourLeases.size
      // Steal leases
      for {
        otherLeases  <- shuffle(allLeases.filterNot(_.owner == workerId))
        leasesToSteal = otherLeases.take(nrLeasesToSteal)
        _             = println(s"$workerId Going to steal ${nrLeasesToSteal} leases: ${leasesToSteal.mkString(",")}")
      } yield leasesToSteal
    } else ZIO.succeed(List.empty)
  }

  /**
   * A single round of lease stealing
   *
   * - First take expired leases (TODO)
   * - Then take X leases from the most loaded worked
   */
  val stealLeases =
    for {
      _             <- UIO(println("Stealing leases"))
      state         <- state.get
      toSteal       <- leasesToSteal(state)
      claimedLeases <- ZIO
                         .foreach(toSteal) { lease =>
                           table.claimLease(lease.copy(owner = Some(workerId)).increaseCounter).asSome.catchAll {
                             case Right(UnableToClaimLease) =>
                               UIO(
                                 println(
                                   s"$workerId Unable to claim lease for shard ${lease.key}, beaten to it by another worker?"
                                 )
                               ).as(None)
                             case Left(e)                   =>
                               ZIO.fail(e)

                           }
                         }
                         .map(_.flatten)
      _             <- ZIO.foreach_(claimedLeases)(registerNewAcquiredLease)
    } yield ()

  // Puts it in the state and the queue
  private def registerNewAcquiredLease(lease: Lease): UIO[Unit] =
    for {
      _         <- UIO(println(s"$workerId Acquired lease: ${lease}"))
      completed <- Promise.make[Nothing, Unit]
      _         <- state.update(_.updateLease(lease).holdLease(lease, completed))
      _         <- acquiredLeasesQueue.offer(lease -> completed)
    } yield ()

  /**
   * Claims all available leases for the current shards (no owner or not existent)
   */
  def initializeLeases: Task[Unit] =
    for {
      state                       <- state.get
      allLeases                    = state.currentLeases
      // Claim new leases for the shards the database doesn't have leases for
      shardsWithoutLease           =
        state.shards.values.toList.filterNot(shard => allLeases.values.map(_.key).toList.contains(shard.shardId()))
      _                           <- UIO(
             println(
               s"No leases exist yet for these shards, creating: ${shardsWithoutLease.map(_.shardId()).mkString(",")}"
             )
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
                                             UIO(
                                               println(
                                                 s"Unable to claim lease for shard ${lease.key}, beaten to it by another worker?"
                                               )
                                             ).as(None)
                                           case Left(e)                   =>
                                             ZIO.fail(e)
                                         }
                                       }
                                       .map(_.flatten)
      // Claim leases without owner
      // TODO we can also claim leases that have expired (worker crashed before releasing)
      claimableLeases              = allLeases.filter { case (key, lease) => lease.owner.isEmpty }
      _                           <- UIO(println(s"Claim of leases without owner for shards ${claimableLeases.mkString(",")}"))
      // _                 <- UIO(println(s"Stealing leases ${leasesToSteal.mkString(",")}"))
      claimedLeases               <- ZIO
                         .foreach(claimableLeases) {
                           case (key, lease) =>
                             table
                               .claimLease(lease.copy(owner = Some(workerId)).increaseCounter)
                               .asSome
                               .catchAll {
                                 case Right(UnableToClaimLease) =>
                                   UIO(
                                     println(
                                       s"Unable to claim lease for shard ${lease.key}, beaten to it by another worker?"
                                     )
                                   ).as(None)
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

  override def makeCheckpointer(shard: Shard) =
    for {
      staged <- Ref.make[Option[Record[_]]](None)
    } yield new Checkpointer {
      override def checkpoint: ZIO[zio.blocking.Blocking, Either[Throwable, ShardLeaseLost.type], Unit] =
        for {
          lastStaged        <- staged.get
          heldLease         <- state.get
                         .map(_.heldLeases.get(shard.shardId()))
                         .someOrFail(Left(new Exception(s"Worker does not hold lease for shard ${shard.shardId()}")))
          (lease, completed) = heldLease
          _                 <- lastStaged match {
                 case Some(r) =>
                   val updatedLease =
                     lease.copy(
                       counter = lease.counter + 1,
                       checkpoint = Some(ExtendedSequenceNumber(r.sequenceNumber, r.subSequenceNumber))
                     )
                   table
                     .updateCheckpoint(updatedLease)
                     .catchAll {
                       case _: ConditionalCheckFailedException =>
                         leaseLost(lease.key) *>
                           ZIO.fail(Right(ShardLeaseLost))
                       case e                                  =>
                         ZIO.fail(Left(e))
                     }
                     .onSuccess(_ => staged.set(None) *> state.update(_.updateLease(updatedLease)))
                 case None    =>
                   ZIO.unit
               }
        } yield ()

      override def stage(r: Record[_]): zio.UIO[Unit] = staged.set(Some(r))
    }

  def releaseLeases: Task[Unit] =
    state.get
      .map(_.heldLeases.values)
      .flatMap(ZIO.foreach_(_)(releaseLease))

  def releaseLease(shard: String) =
    state.get.flatMap(
      _.getHeldLease(shard).map(releaseLease).getOrElse(ZIO.unit)
    )

  private def releaseLease: ((Lease, Promise[Nothing, Unit])) => Task[Unit] = {
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
                (completed.succeed(()).unit, s.releaseLease(lease).updateLease(lease))
            }
            .getOrElse((UIO.unit, s))
      )
      .flatten
  // state.update(_.releaseLease(updatedLease).updateLease(updatedLease))

}

class LeaseTable(client: DynamoDbAsyncClient, applicationName: String) {

  def createLeaseTableIfNotExists: ZIO[Clock, Throwable, Unit] =
    createLeaseTable.unlessM(leaseTableExists)

  // TODO billing mode
  def createLeaseTable: ZIO[Clock, Throwable, Unit] = {
    val keySchema            = List(keySchemaElement("leaseKey", KeyType.HASH))
    val attributeDefinitions = List(attributeDefinition("leaseKey", ScalarAttributeType.S))

    val request = CreateTableRequest
      .builder()
      .tableName(applicationName)
      .billingMode(BillingMode.PAY_PER_REQUEST)
      .keySchema(keySchema.asJavaCollection)
      .attributeDefinitions(attributeDefinitions.asJavaCollection)
      .build()

    val createTable = UIO(println("Creating lease table")) *> asZIO(client.createTable(request))

    // recursion, yeah!
    def awaitTableCreated: ZIO[Clock, Throwable, Unit] =
      leaseTableExists
        .flatMap(awaitTableCreated.delay(10.seconds).unless(_))

    createTable *>
      awaitTableCreated
        .timeoutFail(new Exception("Timeout creating lease table"))(10.minute) // I dunno
  }

  def leaseTableExists: Task[Boolean] =
    asZIO(client.describeTable(DescribeTableRequest.builder().tableName(applicationName).build()))
      .map(_.table().tableStatus() == TableStatus.ACTIVE)
      .catchSome { case _: ResourceNotFoundException => ZIO.succeed(false) }
      .tap(exists => UIO(println(s"Lease table ${applicationName} exists? ${exists}")))

  def getLeasesFromDB: ZIO[Clock, Throwable, List[Lease]] =
    paginatedRequest { (lastItem: Option[DynamoDbItem]) =>
      val builder     = ScanRequest.builder().tableName(applicationName)
      val scanRequest = lastItem.map(_.asJava).fold(builder)(builder.exclusiveStartKey).build()

      asZIO(client.scan(scanRequest)).map { response =>
        val items: Chunk[DynamoDbItem] = Chunk.fromIterable(response.items().asScala).map(_.asScala)

        (items, Option(response.lastEvaluatedKey()).map(_.asScala).filter(_.nonEmpty))
      }

    }(Schedule.forever).flattenChunks.runCollect
      .flatMap(
        ZIO.foreach(_)(i => ZIO.fromTry(toLease(i))).map(_.toList)
      )

  /**
   * Removes the leaseOwner property
   *
   * Expects the given lease's counter - 1
   *
   * @param lease
   * @return
   */
  def releaseLease(lease: Lease): ZIO[Any, Either[Throwable, ShardLeaseLost.type], Unit] = {
    import ImplicitConversions.toAttributeValue
    println(s"Releasing lease: ${lease}")
    val request = UpdateItemRequest
      .builder()
      .tableName(applicationName)
      .key(DynamoDbItem("leaseKey" -> lease.key).asJava)
      .expected(
        Map(
          // TODO we need to update these atomically somehow
          "leaseCounter" -> expectedAttributeValue(lease.counter - 1)
        ).asJava
      )
      .attributeUpdates(Map("leaseOwner" -> deleteAttributeValueUpdate).asJava)
      .build()

    asZIO(client.updateItem(request)).unit.catchAll {
      case e: ConditionalCheckFailedException =>
        ZIO.fail(Right(ShardLeaseLost))
      case e                                  =>
        ZIO.fail(Left(e))
    }.tapError(e => UIO(println(s"Got error releasing lease ${lease.key}: ${e}")))
  }

  // Returns the updated lease
  def claimLease(lease: Lease): ZIO[Any, Either[Throwable, UnableToClaimLease.type], Lease] = {
    println(s"Claim lease: ${lease}")
    val updatedLease =
      lease.copy(
        counter = lease.counter + 1,
        ownerSwitchesSinceCheckpoint = 1L // Is this how this field is used?
      )

    import ImplicitConversions.toAttributeValue
    val request = UpdateItemRequest
      .builder()
      .tableName(applicationName)
      .key(DynamoDbItem("leaseKey" -> lease.key).asJava)
      .expected(
        Map(
          // TODO we need to update these atomically somehow
          "leaseCounter" -> expectedAttributeValue(lease.counter - 1)
        ).asJava
      )
      .attributeUpdates(
        Map(
          "leaseOwner"                   -> putAttributeValueUpdate(updatedLease.owner.get),
          "leaseCounter"                 -> putAttributeValueUpdate(updatedLease.counter),
          "ownerSwitchesSinceCheckpoint" -> putAttributeValueUpdate(updatedLease.ownerSwitchesSinceCheckpoint)
        ).asJava
      )
      .build()

    asZIO(client.updateItem(request)).catchAll {
      case e: ConditionalCheckFailedException =>
        ZIO.fail(Right(UnableToClaimLease))
      case e                                  =>
        ZIO.fail(Left(e))
    }.tapError(e => UIO(println(s"Got error claiming lease: ${e}")))
      .as(updatedLease)

  }

  // Puts the lease counter to the given lease's counter and expects counter - 1
  def updateCheckpoint(lease: Lease) = {
    require(lease.checkpoint.isDefined, "Cannot update checkpoint without Lease.checkpoint property set")
    println(s"Update checkpoint: ${lease}")

    import ImplicitConversions.toAttributeValue

    val request = UpdateItemRequest
      .builder()
      .tableName(applicationName)
      .key(DynamoDbItem("leaseKey" -> lease.key).asJava)
      .expected(
        Map(
          "leaseCounter" -> expectedAttributeValue(lease.counter - 1)
        ).asJava
      )
      .attributeUpdates(
        Map(
          "leaseCounter"                 -> putAttributeValueUpdate(lease.counter),
          "checkpoint"                   -> lease.checkpoint
            .map(_.sequenceNumber)
            .map(putAttributeValueUpdate)
            .getOrElse(putAttributeValueUpdate(null)),
          "checkpointSubSequenceNumber"  -> lease.checkpoint
            .map(_.subSequenceNumber)
            .map(putAttributeValueUpdate)
            .getOrElse(putAttributeValueUpdate(0L)),
          "ownerSwitchesSinceCheckpoint" -> putAttributeValueUpdate(0L)
          // TODO reset pending checkpoint
        ).asJava
      )
      .build()

    asZIO(client.updateItem(request))
      .tapError(e => UIO(println(s"Got error updating checkpoint: ${e}")))
  }

  def createLease(lease: Lease): ZIO[Any, Either[Throwable, LeaseAlreadyExists.type], Unit] = {
    val request = PutItemRequest
      .builder()
      .tableName(applicationName)
      .item(toDynamoItem(lease).asJava)
      .build()

    asZIO(client.putItem(request)).unit
      .mapError(Left(_))
      .tapError(e => UIO(println(s"Got error creating lease: ${e}")))
  }

  private def toLease(item: DynamoDbItem): Try[Lease] =
    Try {
      Lease(
        key = item("leaseKey").s(),
        owner = item.get("leaseOwner").map(_.s()),
        counter = item("leaseCounter").n().toLong,
        ownerSwitchesSinceCheckpoint = item("ownerSwitchesSinceCheckpoint").n().toLong,
        checkpoint = item
          .get("checkpoint")
          .filterNot(_.nul())
          .map(_.s())
          .map(
            ExtendedSequenceNumber(
              _,
              subSequenceNumber = item("checkpointSubSequenceNumber").n().toLong
            )
          ),
        parentShardIds = item.get("parentShardIds").map(_.ss().asScala.toList).getOrElse(List.empty),
        pendingCheckpoint = item
          .get("pendingCheckpoint")
          .map(_.s())
          .map(ExtendedSequenceNumber(_, item("pendingCheckpointSubSequenceNumber").n().toLong))
      )
    }.recoverWith {
      case e =>
        println(s"Error deserializing lease: ${item} ${e}")
        Failure(e)
    }

  import scala.collection.{ mutable => mut }

  private def toDynamoItem(lease: Lease): DynamoDbItem = {
    import DynamoDbUtil.ImplicitConversions.toAttributeValue
    DynamoDbItem(
      "leaseKey"                     -> lease.key,
      "leaseCounter"                 -> lease.counter,
      "checkpoint"                   -> lease.checkpoint.map(_.sequenceNumber).getOrElse(null),
      "checkpointSubSequenceNumber"  -> lease.checkpoint.map(_.subSequenceNumber).getOrElse(null),
      "ownerSwitchesSinceCheckpoint" -> 0L
    ) ++ (if (lease.parentShardIds.nonEmpty) DynamoDbItem("parentShardIds" -> lease.parentShardIds)
          else DynamoDbItem.empty) ++
      lease.owner.fold(DynamoDbItem.empty)(owner => DynamoDbItem("leaseOwner" -> owner))
  }
}

object DynamoDbLeaseCoordinator {

  // TODO incorporate in Record
  case class ExtendedSequenceNumber(sequenceNumber: String, subSequenceNumber: Long)

  case object LeaseAlreadyExists
  case object UnableToClaimLease

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
  }

  def make(
    client: DynamoDbAsyncClient,
    applicationName: String,
    workerId: String,
    shards: Map[String, Shard]
  ): ZManaged[Clock with Random, Throwable, LeaseCoordinator] =
    ZManaged.make {
      val table = new LeaseTable(client, applicationName)
      for {
        _              <- table.createLeaseTableIfNotExists
        currentLeases  <- table.getLeasesFromDB
        leases          = currentLeases.map(l => l.key -> l).toMap
        state          <- Ref.make[State](State(leases, Map.empty, shards))
        acquiredLeases <- Queue.unbounded[(Lease, Promise[Nothing, Unit])]
        coordinator     = new DynamoDbLeaseCoordinator(table, applicationName, workerId, state, acquiredLeases)
        _              <- UIO(println(s"Found ${currentLeases.size} existing leases: " + currentLeases.mkString("\n")))
        _              <- coordinator.initializeLeases

      } yield coordinator
    }(_.releaseLeases.orDie)
      .tap(_.stealLeases.repeat(Schedule.fixed(5.seconds)).forkManaged)

}
