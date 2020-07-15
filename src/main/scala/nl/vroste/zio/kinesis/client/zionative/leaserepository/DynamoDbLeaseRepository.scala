package nl.vroste.zio.kinesis.client.zionative.leaserepository

import java.util.concurrent.TimeoutException

import nl.vroste.zio.kinesis.client.Util.{ asZIO, paginatedRequest }
import nl.vroste.zio.kinesis.client.zionative.LeaseRepository.{
  Lease,
  LeaseAlreadyExists,
  LeaseObsolete,
  UnableToClaimLease
}
import DynamoDbUtil._
import nl.vroste.zio.kinesis.client.zionative.{ ExtendedSequenceNumber, LeaseRepository }
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._
import zio._
import zio.clock.Clock
import zio.duration._
import zio.logging._

import scala.jdk.CollectionConverters._
import scala.util.{ Failure, Try }
import zio.stream.ZStream

// TODO this thing should have a global throttling / backoff
// via a Tap that tries to find the optimal maximal throughput
// See https://degoes.net/articles/zio-challenge
private class DynamoDbLeaseRepository(client: DynamoDbAsyncClient, timeout: Duration) extends LeaseRepository.Service {

  /**
   * Returns whether the table already existed
   */
  override def createLeaseTableIfNotExists(tableName: String): ZIO[Clock with Logging, Throwable, Boolean] = {
    val keySchema            = List(keySchemaElement("leaseKey", KeyType.HASH))
    val attributeDefinitions = List(attributeDefinition("leaseKey", ScalarAttributeType.S))

    val request = CreateTableRequest
      .builder()
      .tableName(tableName)
      .billingMode(BillingMode.PAY_PER_REQUEST)
      .keySchema(keySchema.asJavaCollection)
      .attributeDefinitions(attributeDefinitions.asJavaCollection)
      .build()

    val createTable = log.info(s"Creating lease table ${tableName}") *> asZIO(client.createTable(request)).unit

    def leaseTableExists: ZIO[Logging, Throwable, Boolean] =
      log.debug("Checking if table exists") *>
        asZIO(client.describeTable(DescribeTableRequest.builder().tableName(tableName).build()))
          .map(_.table().tableStatus() == TableStatus.ACTIVE)
          .catchSome { case _: ResourceNotFoundException => ZIO.succeed(false) }
          .tap(exists => log.info(s"Lease table ${tableName} exists? ${exists}"))

    // recursion, yeah!
    def awaitTableCreated: ZIO[Clock with Logging, Throwable, Unit] =
      leaseTableExists
        .flatMap(awaitTableCreated.delay(1.seconds).unless(_))

    // Just try to create the table, if we get ResourceInUse it already existed or another worker is creating it
    (createTable *> awaitTableCreated.as(false)).catchSome {
      // Another worker may have created the table between this worker checking if it exists and attempting to create it
      case _: ResourceInUseException    =>
        ZIO.succeed(true)
      case _: ResourceNotFoundException =>
        // This is for LocalStack compatibility, which returns a ResourceNotFoundException when the table already exists
        // See https://github.com/localstack/localstack/issues/2629
        ZIO.succeed(true)
    }.timeoutFail(new Exception("Timeout creating lease table"))(10.minute) // I dunno
  }

  override def getLeases(tableName: String): ZStream[Clock, Throwable, Lease] =
    // paginatedRequest { (lastItem: Option[DynamoDbItem]) =>
    ZStream.fromEffect {
      val lastItem: Option[DynamoDbItem] = None
      val builder                        = ScanRequest.builder().tableName(tableName)
      val scanRequest                    = lastItem.map(_.asJava).fold(builder)(builder.exclusiveStartKey).build()

      asZIO(client.scan(scanRequest)).map { response =>
        val items: Chunk[DynamoDbItem] = Chunk.fromIterable(response.items().asScala).map(_.asScala)

        (items, Option(response.lastEvaluatedKey()).map(_.asScala).filter(_.nonEmpty))
      }.timed.map {
        case (timing, result) =>
          println(s"GetLeases took ${timing.toMillis}")
          result
      }.map(_._1)

      // }(Schedule.forever)
    }.flattenChunks.mapM(l => ZIO.fromTry(toLease(l)))

  /**
   * Removes the leaseOwner property
   *
   * Expects the given lease's counter - 1
   *
   * @param lease
   * @return
   */
  override def releaseLease(
    tableName: String,
    lease: Lease
  ): ZIO[Logging, Either[Throwable, LeaseObsolete.type], Unit] = {
    import ImplicitConversions.toAttributeValue
    val request = UpdateItemRequest
      .builder()
      .tableName(tableName)
      .key(DynamoDbItem("leaseKey" -> lease.key).asJava)
      .expected(
        Map(
          "leaseCounter" -> expectedAttributeValue(lease.counter - 1)
        ).asJava
      )
      .attributeUpdates(
        Map(
          "leaseOwner" -> deleteAttributeValueUpdate
        ).asJava
      )
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
  override def claimLease(
    tableName: String,
    lease: Lease
  ): ZIO[Logging with Clock, Either[Throwable, UnableToClaimLease.type], Unit] = {
    import ImplicitConversions.toAttributeValue
    val request = UpdateItemRequest
      .builder()
      .tableName(tableName)
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
          "ownerSwitchesSinceCheckpoint" -> putAttributeValueUpdate(0L) // Just for KCL compatibility
        ).asJava
      )
      .build()

    asZIO(client.updateItem(request))
      .timeoutFail(new TimeoutException(s"Timeout claiming lease"))(timeout)
      // .tapError(e => log.warn(s"Got error claiming lease: ${e}"))
      .unit
      .catchAll {
        case _: ConditionalCheckFailedException =>
          ZIO.fail(Right(UnableToClaimLease))
        case e                                  =>
          ZIO.fail(Left(e))
      }

  }

  // Puts the lease counter to the given lease's counter and expects counter - 1
  override def updateCheckpoint(
    tableName: String,
    lease: Lease
  ): ZIO[Logging with Clock, Either[Throwable, LeaseObsolete.type], Unit] = {
    require(lease.checkpoint.isDefined, "Cannot update checkpoint without Lease.checkpoint property set")

    import ImplicitConversions.toAttributeValue

    val request = UpdateItemRequest
      .builder()
      .tableName(tableName)
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
          "ownerSwitchesSinceCheckpoint" -> putAttributeValueUpdate(0L) // Just for KCL compatibility
        ).asJava
      )
      .build()

    asZIO(client.updateItem(request))
      .timeoutFail(new TimeoutException(s"Timeout updating checkpoint"))(timeout)
      .unit
      .catchAll {
        case _: ConditionalCheckFailedException =>
          ZIO.fail(Right(LeaseObsolete))
        case e                                  =>
          ZIO.fail(Left(e))
      }
  }

  override def renewLease(
    tableName: String,
    lease: Lease
  ): ZIO[Logging with Clock, Either[Throwable, LeaseObsolete.type], Unit] = {
    import ImplicitConversions.toAttributeValue

    val request = UpdateItemRequest
      .builder()
      .tableName(tableName)
      .key(DynamoDbItem("leaseKey" -> lease.key).asJava)
      .expected(
        Map(
          "leaseCounter" -> expectedAttributeValue(lease.counter - 1)
        ).asJava
      )
      .attributeUpdates(Map("leaseCounter" -> putAttributeValueUpdate(lease.counter)).asJava)
      .build()

    asZIO(client.updateItem(request))
      .timeoutFail(new TimeoutException(s"Timeout renewing lease"))(timeout)
      .tapError(e => log.warn(s"Got error updating lease: ${e}"))
      .unit
      .catchAll {
        case _: ConditionalCheckFailedException =>
          ZIO.fail(Right(LeaseObsolete))
        case e                                  =>
          ZIO.fail(Left(e))
      }
  }

  override def createLease(
    tableName: String,
    lease: Lease
  ): ZIO[Logging with Clock, Either[Throwable, LeaseAlreadyExists.type], Unit] = {
    val request = PutItemRequest
      .builder()
      .tableName(tableName)
      .item(toDynamoItem(lease).asJava)
      .conditionExpression("attribute_not_exists(leaseKey)")
      .build()

    asZIO(client.putItem(request))
      .timeoutFail(new TimeoutException(s"Timeout creating lease"))(timeout)
      .unit
      .mapError {
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
        parentShardIds = item.get("parentShardIds").map(_.ss().asScala.toList).getOrElse(List.empty)
      )
    }.recoverWith {
      case e =>
        println(s"Error deserializing lease: ${item} ${e}")
        Failure(e)
    }

  private def toDynamoItem(lease: Lease): DynamoDbItem = {
    import DynamoDbUtil.ImplicitConversions.toAttributeValue
    DynamoDbItem(
      "leaseKey"                    -> lease.key,
      "leaseCounter"                -> lease.counter,
      "checkpoint"                  -> lease.checkpoint.map(_.sequenceNumber).getOrElse(null),
      "checkpointSubSequenceNumber" -> lease.checkpoint.map(_.subSequenceNumber).getOrElse(null)
    ) ++ (if (lease.parentShardIds.nonEmpty) DynamoDbItem("parentShardIds" -> lease.parentShardIds)
          else DynamoDbItem.empty) ++
      lease.owner.fold(DynamoDbItem.empty)(owner => DynamoDbItem("leaseOwner" -> owner))
  }
}

object DynamoDbLeaseRepository {
  val defaultTimeout = 10.seconds

  val live: ZLayer[Has[DynamoDbAsyncClient], Nothing, LeaseRepository] = make(defaultTimeout)

  def make(timeout: Duration = defaultTimeout): ZLayer[Has[DynamoDbAsyncClient], Nothing, LeaseRepository] =
    ZLayer.fromService(new DynamoDbLeaseRepository(_, timeout))

}
