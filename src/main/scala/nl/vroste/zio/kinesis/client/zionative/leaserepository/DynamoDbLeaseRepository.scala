package nl.vroste.zio.kinesis.client.zionative.leaserepository

import java.util.concurrent.TimeoutException

import DynamoDbUtil._
import io.github.vigoo.zioaws.dynamodb.model._
import io.github.vigoo.zioaws.dynamodb.{ model, DynamoDb }
import nl.vroste.zio.kinesis.client.zionative.LeaseRepository.{
  Lease,
  LeaseAlreadyExists,
  LeaseObsolete,
  UnableToClaimLease
}
import nl.vroste.zio.kinesis.client.zionative.{ ExtendedSequenceNumber, LeaseRepository }
import software.amazon.awssdk.services.dynamodb.model.{
  ConditionalCheckFailedException,
  ResourceInUseException,
  ResourceNotFoundException
}
import zio._
import zio.clock.Clock
import zio.duration._
import zio.logging._
import zio.stream.ZStream

import scala.util.{ Failure, Try }

// TODO this thing should have a global throttling / backoff
// via a Tap that tries to find the optimal maximal throughput
// See https://degoes.net/articles/zio-challenge
private class DynamoDbLeaseRepository(client: DynamoDb.Service, timeout: Duration) extends LeaseRepository.Service {

  /**
   * Returns whether the table already existed
   */
  override def createLeaseTableIfNotExists(tableName: String): ZIO[Clock with Logging, Throwable, Boolean] = {
    val keySchema            = List(keySchemaElement("leaseKey", KeyType.HASH))
    val attributeDefinitions = List(attributeDefinition("leaseKey", ScalarAttributeType.S))

    val request = CreateTableRequest(
      tableName = tableName,
      billingMode = Some(BillingMode.PAY_PER_REQUEST),
      keySchema = keySchema,
      attributeDefinitions = attributeDefinitions
    )

    val createTable =
      log.info(s"Creating lease table ${tableName}") *> client.createTable(request).mapError(_.toThrowable).unit

    def describeTable: Task[model.TableStatus] =
      client
        .describeTable(DescribeTableRequest(tableName))
        .flatMap(_.table)
        .flatMap(_.tableStatus)
        .mapError(_.toThrowable)

    def leaseTableExists: ZIO[Logging, Throwable, Boolean] =
      log.debug(s"Checking if lease table '${tableName}' exists and is active") *>
        describeTable
          .map(s => s == TableStatus.ACTIVE || s == TableStatus.UPDATING)
          .catchSome { case _: ResourceNotFoundException => ZIO.succeed(false) }
          .tap(exists => log.info(s"Lease table ${tableName} exists? ${exists}"))

    // recursion, yeah!
    def awaitTableActive: ZIO[Clock with Logging, Throwable, Unit] =
      leaseTableExists
        .flatMap(awaitTableActive.delay(1.seconds).unless(_))

    // Optimistically assume the table already exists
    log.debug(s"Checking if lease table '${tableName}' exists") *>
      describeTable.flatMap {
        case TableStatus.ACTIVE | TableStatus.UPDATING =>
          log.debug(s"Lease table '${tableName}' exists and is active") *>
            ZIO.succeed(true)
        case TableStatus.CREATING                      =>
          log.debug(s"Lease table '${tableName}' has CREATING status") *>
            awaitTableActive.delay(1.seconds).as(true)
        case s @ _                                     =>
          ZIO.fail(new Exception(s"Could not create lease table '${tableName}'. Invalid table status: ${s}"))
      }.catchSome {
        case _: ResourceNotFoundException =>
          createTable.catchSome {
            // Race condition: another worker is creating the table at the same time as we are, we lose
            case _: ResourceInUseException =>
              ZIO.unit
          } *> awaitTableActive.delay(1.second).as(false)
      }.timeoutFail(new Exception("Timeout creating lease table"))(1.minute)
  }

  override def getLeases(tableName: String): ZStream[Clock, Throwable, Lease] =
    client
      .scan(ScanRequest(tableName))
      .mapError(_.toThrowable)
      .map(item => toLease(item.view.mapValues(_.editable).toMap))
      .mapM(ZIO.fromTry(_))

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
    val request = baseUpdateItemRequestForLease(tableName, lease).copy(
      attributeUpdates = Some(Map("leaseOwner" -> deleteAttributeValueUpdate))
    )

    client
      .updateItem(request)
      .mapError(_.toThrowable)
      .unit
      .catchAll {
        case e: ConditionalCheckFailedException =>
          log.error("Check failed", Cause.fail(e))
          ZIO.fail(Right(LeaseObsolete))
        case e                                  =>
          ZIO.fail(Left(e))
      }
      .tapError(e => log.info(s"Got error releasing lease ${lease.key}: ${e}"))
  }

  // Returns the updated lease
  override def claimLease(
    tableName: String,
    lease: Lease
  ): ZIO[Logging with Clock, Either[Throwable, UnableToClaimLease.type], Unit] = {
    val request = baseUpdateItemRequestForLease(tableName, lease).copy(
      attributeUpdates = Some(
        Map(
          "leaseOwner"                   -> putAttributeValueUpdate(lease.owner.get),
          "leaseCounter"                 -> putAttributeValueUpdate(lease.counter),
          "ownerSwitchesSinceCheckpoint" -> putAttributeValueUpdate(0L) // Just for KCL compatibility
        )
      )
    )

    client
      .updateItem(request)
      .mapError(_.toThrowable)
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

  private def baseUpdateItemRequestForLease(
    tableName: String,
    lease: Lease
  ) = {
    import ImplicitConversions.toAttributeValue
    UpdateItemRequest(
      tableName,
      key = DynamoDbItem("leaseKey" -> lease.key),
      expected = Some(Map("leaseCounter" -> expectedAttributeValue(lease.counter - 1)))
    )
  }
// Puts the lease counter to the given lease's counter and expects counter - 1
  override def updateCheckpoint(
    tableName: String,
    lease: Lease
  ): ZIO[Logging with Clock, Either[Throwable, LeaseObsolete.type], Unit] = {
    require(lease.checkpoint.isDefined, "Cannot update checkpoint without Lease.checkpoint property set")

    val request = baseUpdateItemRequestForLease(tableName, lease).copy(
      attributeUpdates = Some(
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
        )
      )
    )

    client
      .updateItem(request)
      .mapError(_.toThrowable)
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

    val request = baseUpdateItemRequestForLease(tableName, lease)
      .copy(attributeUpdates = Some(Map("leaseCounter" -> putAttributeValueUpdate(lease.counter))))

    client
      .updateItem(request)
      .mapError(_.toThrowable)
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
    val request =
      PutItemRequest(tableName, toDynamoItem(lease), conditionExpression = Some("attribute_not_exists(leaseKey)"))

    client
      .putItem(request)
      .mapError(_.toThrowable)
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
        key = item("leaseKey").s.get,
        owner = item.get("leaseOwner").flatMap(_.s),
        counter = item("leaseCounter").n.get.toLong,
        checkpoint = item
          .get("checkpoint")
          .filterNot(_.nul.isDefined)
          .flatMap(_.s)
          .map(
            ExtendedSequenceNumber(
              _,
              subSequenceNumber = item("checkpointSubSequenceNumber").n.get.toLong
            )
          ),
        parentShardIds = item.get("parentShardIds").map(_.ss.toList.flatten).getOrElse(List.empty)
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

  val live: ZLayer[DynamoDb, Nothing, LeaseRepository] = make(defaultTimeout)

  def make(timeout: Duration = defaultTimeout): ZLayer[DynamoDb, Nothing, LeaseRepository] =
    ZLayer.fromService(new DynamoDbLeaseRepository(_, timeout))

}
