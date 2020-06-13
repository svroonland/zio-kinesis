package nl.vroste.zio.kinesis.client.zionative.dynamodb

import nl.vroste.zio.kinesis.client.DynamicConsumer.{ Checkpointer, Record }
import nl.vroste.zio.kinesis.client.Util.asZIO
import nl.vroste.zio.kinesis.client.zionative.LeaseCoordinator
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
import nl.vroste.zio.kinesis.client.zionative.dynamodb.DynamoDbLeaseCoordinator.ShardLeaseLost
import nl.vroste.zio.kinesis.client.zionative.dynamodb.DynamoDbLeaseCoordinator.ExtendedSequenceNumber

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
  currentLeases: Ref[Map[String, Lease]],
  heldLeases: Ref[Map[String, (Lease, Promise[Nothing, Unit])]],
  shards: Map[String, Shard]
) extends LeaseCoordinator {

  import DynamoDbLeaseCoordinator._
  import ZioExtensions.OnSuccessSyntax

  /**
   * Claims all available leases for the current shards (no owner or not existent)
   *
    * @return List of leases + signal for lease lost
   */
  def initialLeases: Task[List[(Lease, Promise[Nothing, Unit])]] =
    for {
      allLeases         <- currentLeases.get
      // Claim new leases for the shards the database doesn't have leases for
      shardsWithoutLease =
        shards.values.toList.filterNot(shard => allLeases.values.map(_.key).toList.contains(shard.shardId()))
      _                 <- UIO(println(s"No leases exist yet for these shards, creating: ${shardsWithoutLease.mkString(",")}"))
      _                 <- ZIO.foreach(shardsWithoutLease) { shard =>
             val lease = Lease(
               key = shard.shardId(),
               owner = Some(workerId),
               counter = 0L,
               ownerSwitchesSinceCheckpoint = 0L,
               checkpoint = None,
               parentShardIds = Seq.empty
             )

             (table.createLease(lease) *> currentLeases.update(_ + (lease.key -> lease)).as(lease)).catchAll {
               case Right(LeaseAlreadyExists) =>
                 UIO(
                   println(
                     s"Unable to claim lease for shard ${lease.key}, beaten to it by another worker?"
                   )
                 )
               case Left(e)                   =>
                 ZIO.fail(e)
             }
           }
      // Claim leases without owner
      claimableLeases    = allLeases.filter { case (key, lease) => lease.owner.isEmpty }
      leasesToSteal      = {
        val allWorkers     = allLeases.values.map(_.owner).collect { case Some(owner) => owner }.toSet ++ Set(workerId)
        val nrLeasesToHave = Math.floor(shards.size * 1.0 / (allWorkers.size * 1.0))
      }
      _                 <- UIO(println(s"Claim of leases without owner for shards ${claimableLeases.mkString(",")}"))
      // _                 <- UIO(println(s"Stealing leases ${leasesToSteal.mkString(",")}"))
      _                 <- ZIO.foreach(claimableLeases) { // ++ leasesToSteal) {
             case (key, lease) =>
               table
                 .claimLease(lease.copy(owner = Some(workerId)))
                 .flatMap { updatedLease =>
                   currentLeases.update(_ + (lease.key -> updatedLease)).as(updatedLease)
                 }
                 .catchAll {
                   case Right(UnableToClaimLease) =>
                     UIO(
                       println(
                         s"Unable to claim lease for shard ${lease.key}, beaten to it by another worker?"
                       )
                     )
                   case Left(e)                   =>
                     ZIO.fail(e)
                 }
           }
      claimedLeases     <- currentLeases.get.map(_.values.toList.filter(_.owner.contains(workerId)))
      newHeldLeases     <- ZIO.foreach(claimedLeases)(lease => Promise.make[Nothing, Unit].map(lease.key -> (lease, _)))
      _                 <- heldLeases.set(newHeldLeases.toMap)
      newAllLeases      <- currentLeases.get
      _                  = println(s"Claimed ${newHeldLeases.size}/${newAllLeases.size} leases for worker ${workerId}")

    } yield newHeldLeases.toMap.values.toList

  override def acquiredLeases: ZStream[zio.clock.Clock, Throwable, AcquiredLease] =
    ZStream
      .fromEffect(initialLeases)
      .mapConcat(identity(_))
      .map {
        case (lease, complete) => AcquiredLease(lease.key, complete)
      } ++ ZStream.never // To prevent stream from closing

  override def getCheckpointForShard(shard: Shard): UIO[Option[ExtendedSequenceNumber]] =
    for {
      leaseOpt <- currentLeases.get
                    .map(_.get(shard.shardId()))
    } yield leaseOpt.flatMap(_.checkpoint)

  override def makeCheckpointer(shard: Shard) =
    for {
      lease  <- currentLeases.get
                 .map(_.get(shard.shardId()))
                 .someOrFail(new Exception(s"Worker does not hold lease for shard ${shard.shardId()}"))
      staged <- Ref.make[Option[Record[_]]](None)
    } yield new Checkpointer {
      override def checkpoint: ZIO[zio.blocking.Blocking, Throwable, Unit] =
        for {
          lastStaged <- staged.get
          _          <- lastStaged match {
                 case Some(r) =>
                   val updatedLease =
                     lease.copy(
                       counter = lease.counter + 1,
                       checkpoint = Some(ExtendedSequenceNumber(r.sequenceNumber, r.subSequenceNumber))
                     )
                   table
                     .updateCheckpoint(
                       updatedLease
                       // TODO handle lease lost
                     )
                     .onSuccess(_ => staged.set(None) *> currentLeases.update(_ + (lease.key -> updatedLease)))
                 case None    =>
                   ZIO.unit
               }
        } yield ()

      override def stage(r: Record[_]): zio.UIO[Unit] = staged.set(Some(r))
    }

  def releaseLeases =
    currentLeases.get
      .map(_.values)
      .flatMap(
        ZIO.foreach(_)(table.releaseLease(_).catchAll {
          case Right(ShardLeaseLost) => // This is fine at shutdown
            ZIO.unit
          case Left(e)               =>
            ZIO.fail(e)
        })
      )
      .unit
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
   * Expects the given lease's counter
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
          "leaseCounter" -> expectedAttributeValue(lease.counter)
        ).asJava
      )
      .attributeUpdates(Map("leaseOwner" -> deleteAttributeValueUpdate).asJava)
      .build()

    asZIO(client.updateItem(request)).unit.catchAll {
      case e: ConditionalCheckFailedException =>
        ZIO.fail(Right(ShardLeaseLost))
      case e                                  =>
        ZIO.fail(Left(e))
    }.tapError(e => UIO(println(s"Got error releasing lease: ${e}")))
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
          "leaseCounter" -> expectedAttributeValue(lease.counter)
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
    println(s"Creating lease ${lease}")
    val request = PutItemRequest
      .builder()
      .tableName(applicationName)
      .item(toDynamoItem(lease).asJava)
      .build()
    println(request)

    asZIO(client.putItem(request)).unit
      .mapError(Left(_))
      .tapError(e => UIO(println(s"Got error creating lease: ${e}")))
  }

  private def toLease(item: DynamoDbItem): Try[Lease] =
    Try {
      println(s"Deserializing ${item}")
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

  case object ShardLeaseLost
  case object LeaseAlreadyExists
  case object UnableToClaimLease

  // TODO some of these fields are probably optional
  case class Lease(
    key: String,
    owner: Option[String],
    counter: Long,
    ownerSwitchesSinceCheckpoint: Long,
    checkpoint: Option[ExtendedSequenceNumber],
    parentShardIds: Seq[String],
    pendingCheckpoint: Option[ExtendedSequenceNumber] = None
  )

  def make(
    client: DynamoDbAsyncClient,
    applicationName: String,
    workerId: String,
    shards: Map[String, Shard]
  ): ZManaged[Clock, Throwable, LeaseCoordinator] =
    ZManaged.make {
      for {
        heldLeases    <- Ref.make[Map[String, (Lease, Promise[Nothing, Unit])]](Map.empty)
        table          = new LeaseTable(client, applicationName)
        _             <- table.createLeaseTableIfNotExists
        currentLeases <- table.getLeasesFromDB
        leases        <- Ref.make[Map[String, Lease]](currentLeases.map(l => l.key -> l).toMap)
        coordinator    = new DynamoDbLeaseCoordinator(table, applicationName, workerId, leases, heldLeases, shards)
        _             <- UIO(println(s"Found ${currentLeases.size} existing leases: " + currentLeases.mkString("\n")))
      } yield coordinator
    }(_.releaseLeases.orDie)

}
