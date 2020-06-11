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
 * - The lease owner is the worker identifier (?)
 * - The checkpoint is the checkpoint
 * - The lease counter is an atomically updated counter to prevent concurrent changes
 * - Owner switches since checkpoint? Not sure what useful for.. TODO
 * - Parent shard IDs: not yet handled TODO
 * - pending checkpoint: not yet supported TODO
 *
 *
 * TODO for now only single worker model checkpointing is supported
 */
private class DynamoDbLeaseCoordinator(
  client: DynamoDbAsyncClient,
  applicationName: String,
  currentLeases: Ref[Map[String, Lease]]
) extends LeaseCoordinator {
  import DynamoDbLeaseCoordinator._
  import ZioExtensions.OnSuccessSyntax

  override def getCheckpointForShard(shard: Shard): UIO[Option[ExtendedSequenceNumber]] =
    for {
      leaseOpt <- currentLeases.get
                    .map(_.get(shard.shardId()))
    } yield leaseOpt.flatMap(_.checkpoint)

  override def makeCheckpointer(shard: Shard) =
    for {
      leaseOpt <- currentLeases.get
                    .map(_.get(shard.shardId()))
      lease    <- leaseOpt.map(ZIO.succeed(_)).getOrElse {
                 val lease = Lease(
                   key = shard.shardId(),
                   owner = workerId,
                   counter = 0L,
                   ownerSwitchesSinceCheckpoint = 0L,
                   checkpoint = None,
                   parentShardIds = Seq.empty
                 )
                 // TODO CreateLease may fail but we should handle that before trying to process the shard
                 createLease(lease).ignore *> currentLeases.update(_ + (lease.key -> lease)).as(lease)
               }
      staged   <- Ref.make[Option[Record[_]]](None)
    } yield new Checkpointer {
      override def checkpoint: ZIO[zio.blocking.Blocking, Throwable, Unit] =
        for {
          lastStaged <- staged.get
          _          <- lastStaged match {
                 case Some(r) =>
                   val newLease =
                     lease.copy(
                       counter = lease.counter + 1,
                       checkpoint = Some(ExtendedSequenceNumber(r.sequenceNumber, r.subSequenceNumber))
                     )
                   updateCheckpoint(
                     newLease
                     // TODO handle lease lost
                   ).onSuccess(_ => staged.set(None) *> currentLeases.update(_ + (lease.key -> newLease)))
                 case None    =>
                   ZIO.unit
               }
        } yield ()

      override def stage(r: Record[_]): zio.UIO[Unit] = staged.set(Some(r))
    }

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

  def getLeases: ZIO[Clock, Throwable, List[Lease]] =
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

  def releaseLeases = currentLeases.get.flatMap(leases => ZIO.foreach(leases.values)(releaseLease)).unit

  def releaseLease(lease: Lease): ZIO[Any, Either[Throwable, ShardLeaseLost.type], Unit] = {
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
      .attributeUpdates(Map("leaseOwner" -> deleteAttributeValueUpdate).asJava)
      .build()

    asZIO(client.updateItem(request)).unit.catchAll {
      case e: ConditionalCheckFailedException =>
        ZIO.fail(Right(ShardLeaseLost))
      case e                                  =>
        ZIO.fail(Left(e))
    }.tapError(e => UIO(println(s"Got error releasing lease: ${e}")))
  }

  def updateCheckpoint(lease: Lease) = {
    require(lease.checkpoint.isDefined, "Cannot update checkpoint without Lease.checkpoint property set")

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
}

object DynamoDbLeaseCoordinator {

  // TODO incorporate in Record
  case class ExtendedSequenceNumber(sequenceNumber: String, subSequenceNumber: Long)

  case object ShardLeaseLost
  case object LeaseAlreadyExists

  // TODO some of these fields are probably optional
  case class Lease(
    key: String,
    owner: String,
    counter: Long,
    ownerSwitchesSinceCheckpoint: Long,
    checkpoint: Option[ExtendedSequenceNumber],
    parentShardIds: Seq[String],
    pendingCheckpoint: Option[ExtendedSequenceNumber] = None
  )

  def make(client: DynamoDbAsyncClient, applicationName: String): ZManaged[Clock, Throwable, LeaseCoordinator] =
    ZManaged.make {
      for {
        leases        <- Ref.make[Map[String, Lease]](Map.empty)
        coordinator    = new DynamoDbLeaseCoordinator(client, applicationName, leases)
        _             <- coordinator.createLeaseTableIfNotExists
        currentLeases <- coordinator.getLeases
        _             <- UIO(println(s"Found ${currentLeases.size} existing leases: " + currentLeases.mkString("\n")))
        _             <- leases.set(currentLeases.map(l => l.key -> l).toMap)
      } yield coordinator
    }(_.releaseLeases.catchAll {
      case Right(ShardLeaseLost) => // This is fine at shutdown
        ZIO.unit
      case Left(e)               =>
        ZIO.fail(e)
    }.orDie)

  private def toLease(item: DynamoDbItem): Try[Lease] =
    Try {
      Lease(
        key = item("leaseKey").s(),
        owner = item("leaseOwner").s(),
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
      "leaseOwner"                   -> workerId,
      "leaseCounter"                 -> lease.counter,
      "checkpoint"                   -> lease.checkpoint.map(_.sequenceNumber).getOrElse(null),
      "checkpointSubSequenceNumber"  -> lease.checkpoint.map(_.subSequenceNumber).getOrElse(null),
      "ownerSwitchesSinceCheckpoint" -> attributeValue(0L)
    ) ++ (if (lease.parentShardIds.nonEmpty) DynamoDbItem("parentShardIds" -> lease.parentShardIds)
          else DynamoDbItem.empty)
  }

  val workerId = "single-worker-app"

}
