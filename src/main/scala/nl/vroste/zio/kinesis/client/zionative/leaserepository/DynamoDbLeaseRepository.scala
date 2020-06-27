package nl.vroste.zio.kinesis.client.zionative.leaserepository

import nl.vroste.zio.kinesis.client.Util.{ asZIO, paginatedRequest }
import nl.vroste.zio.kinesis.client.zionative.LeaseRepository.{ LeaseAlreadyExists, LeaseObsolete, UnableToClaimLease }
import nl.vroste.zio.kinesis.client.zionative.leasecoordinator.DefaultLeaseCoordinator.Lease
import DynamoDbUtil._
import nl.vroste.zio.kinesis.client.zionative.{ ExtendedSequenceNumber, LeaseRepository, LeaseRepositoryFactory }
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._
import zio._
import zio.clock.Clock
import zio.duration._
import zio.logging._

import scala.jdk.CollectionConverters._
import scala.util.{ Failure, Try }

private class DynamoDbLeaseRepository(client: DynamoDbAsyncClient, applicationName: String)
    extends LeaseRepository.Service {

  /**
   * Returns whether the table already existed
   */
  override def createLeaseTableIfNotExists: ZIO[Clock with Logging, Throwable, Boolean] = {
    val keySchema            = List(keySchemaElement("leaseKey", KeyType.HASH))
    val attributeDefinitions = List(attributeDefinition("leaseKey", ScalarAttributeType.S))

    val request = CreateTableRequest
      .builder()
      .tableName(applicationName)
      .billingMode(BillingMode.PAY_PER_REQUEST)
      .keySchema(keySchema.asJavaCollection)
      .attributeDefinitions(attributeDefinitions.asJavaCollection)
      .build()

    val createTable = log.info(s"Creating lease table ${applicationName}") *> asZIO(client.createTable(request))
    // This is for LocalStack compatibility, which returns an empty response when the table already exists
    // See https://github.com/localstack/localstack/issues/2629
      .filterOrFail(_.tableDescription() != null)(
        ResourceInUseException.builder().message("Table already exists").build()
      )
      .unit

    // recursion, yeah!
    def awaitTableCreated: ZIO[Clock with Logging, Throwable, Unit] =
      leaseTableExists
        .flatMap(awaitTableCreated.delay(1.seconds).unless(_)) // TODO 10 seconds?

    // Just try to create the table, if we get ResourceInUse it already existed or another worker is creating it
    (createTable *> awaitTableCreated.as(false)).catchSome {
      // Another worker may have created the table between this worker checking if it exists and attempting to create it
      case _: ResourceInUseException =>
        ZIO.succeed(true)
    }.timeoutFail(new Exception("Timeout creating lease table"))(10.minute) // I dunno
  }

  override def leaseTableExists: ZIO[Logging, Throwable, Boolean] =
    log.debug("Checking if table exists") *>
      asZIO(client.describeTable(DescribeTableRequest.builder().tableName(applicationName).build()))
        .map(_.table().tableStatus() == TableStatus.ACTIVE)
        .catchSome { case _: ResourceNotFoundException => ZIO.succeed(false) }
        .tap(exists => log.info(s"Lease table ${applicationName} exists? ${exists}"))

  override def getLeases: ZIO[Clock, Throwable, List[Lease]] =
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
  override def releaseLease(lease: Lease): ZIO[Logging, Either[Throwable, LeaseObsolete.type], Unit] = {
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
      .attributeUpdates(Map("leaseOwner" -> deleteAttributeValueUpdate).asJava)
      .build()

    asZIO(client.updateItem(request)).unit.catchAll {
      case e: ConditionalCheckFailedException =>
        log.error("Check failed", Cause.fail(e))
        ZIO.fail(Right(LeaseObsolete))
      case e                                  =>
        ZIO.fail(Left(e))
    }.tapError(e => log.info(s"Got error releasing lease ${lease.key}: ${e}"))
  }

  // Returns the updated lease
  override def claimLease(lease: Lease): ZIO[Logging, Either[Throwable, UnableToClaimLease.type], Unit] = {
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
          "leaseOwner"                   -> putAttributeValueUpdate(lease.owner.get),
          "leaseCounter"                 -> putAttributeValueUpdate(lease.counter),
          "ownerSwitchesSinceCheckpoint" -> putAttributeValueUpdate(lease.ownerSwitchesSinceCheckpoint)
        ).asJava
      )
      .build()

    asZIO(client.updateItem(request))
    // .tapError(e => log.warn(s"Got error claiming lease: ${e}"))
    .unit.catchAll {
      case _: ConditionalCheckFailedException =>
        ZIO.fail(Right(UnableToClaimLease))
      case e                                  =>
        ZIO.fail(Left(e))
    }

  }

  // Puts the lease counter to the given lease's counter and expects counter - 1
  override def updateCheckpoint(lease: Lease): ZIO[Logging, Either[Throwable, LeaseObsolete.type], Unit] = {
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
          "leaseOwner"                   -> lease.owner.map(putAttributeValueUpdate(_)).getOrElse(deleteAttributeValueUpdate),
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
        ).asJava
      )
      .build()

    asZIO(client.updateItem(request)).unit.catchAll {
      case _: ConditionalCheckFailedException =>
        ZIO.fail(Right(LeaseObsolete))
      case e                                  =>
        ZIO.fail(Left(e))
    }
  }

  override def renewLease(lease: Lease): ZIO[Logging, Either[Throwable, LeaseObsolete.type], Unit] = {
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
      .attributeUpdates(Map("leaseCounter" -> putAttributeValueUpdate(lease.counter)).asJava)
      .build()

    asZIO(client.updateItem(request))
      .tapError(e => log.warn(s"Got error updating lease: ${e}"))
      .unit
      .catchAll {
        case _: ConditionalCheckFailedException =>
          ZIO.fail(Right(LeaseObsolete))
        case e                                  =>
          ZIO.fail(Left(e))
      }
  }

  override def createLease(lease: Lease): ZIO[Logging, Either[Throwable, LeaseAlreadyExists.type], Unit] = {
    val request = PutItemRequest
      .builder()
      .tableName(applicationName)
      .item(toDynamoItem(lease).asJava)
      .conditionExpression("attribute_not_exists(leaseKey)")
      .build()

    asZIO(client.putItem(request)).unit.mapError {
      case _: ConditionalCheckFailedException =>
        Right(LeaseAlreadyExists)
      case e                                  =>
        Left(e)
    }
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

object DynamoDbLeaseRepository {

  val factory: ZLayer[Has[DynamoDbAsyncClient], Nothing, LeaseRepositoryFactory] =
    ZLayer.fromService(client => applicationName => new DynamoDbLeaseRepository(client, applicationName))
}
