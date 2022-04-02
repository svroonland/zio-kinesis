package nl.vroste.zio.kinesis.client.zionative.leaserepository

import nl.vroste.zio.kinesis.client.zionative.LeaseRepository.{
  Lease,
  LeaseAlreadyExists,
  LeaseObsolete,
  UnableToClaimLease
}
import nl.vroste.zio.kinesis.client.zionative.leaserepository.DynamoDbUtil._
import nl.vroste.zio.kinesis.client.zionative.{ ExtendedSequenceNumber, LeaseRepository, SpecialCheckpoint }
import software.amazon.awssdk.services.dynamodb.model.{
  ConditionalCheckFailedException,
  ResourceInUseException,
  ResourceNotFoundException
}
import zio.{ Clock, _ }
import zio.aws.dynamodb.model._
import zio.aws.dynamodb.model.primitives.{ AttributeName, ConditionExpression, TableName }
import zio.aws.dynamodb.{ model, DynamoDb }
import zio.stream.ZStream

import java.util.concurrent.TimeoutException
import scala.util.{ Failure, Try }
import scala.collection.compat._

// TODO this thing should have a global throttling / backoff
// via a Tap that tries to find the optimal maximal throughput
// See https://degoes.net/articles/zio-challenge
private class DynamoDbLeaseRepository(client: DynamoDb, timeout: Duration) extends LeaseRepository.Service {

  /**
   * Returns whether the table already existed
   */
  override def createLeaseTableIfNotExists(tableName: String): ZIO[Clock, Throwable, Boolean] = {
    val keySchema            = List(keySchemaElement("leaseKey", KeyType.HASH))
    val attributeDefinitions = List(attributeDefinition("leaseKey", ScalarAttributeType.S))

    val request = model.CreateTableRequest(
      tableName = TableName(tableName),
      billingMode = Some(BillingMode.PAY_PER_REQUEST),
      keySchema = keySchema,
      attributeDefinitions = attributeDefinitions
    )

    val createTable =
      ZIO.logInfo(s"Creating lease table ${tableName}") *> client
        .createTable(request)
        .mapError(_.toThrowable)
        .unit

    // TODO should be Option[TableStatus]
    def describeTable: Task[model.TableStatus] =
      client
        .describeTable(DescribeTableRequest(TableName(tableName)))
        .flatMap(_.getTable)
        .flatMap(_.getTableStatus)
        .mapError(_.toThrowable)

    def leaseTableExists: ZIO[Any, Throwable, Boolean] =
      ZIO.logDebug(s"Checking if lease table '${tableName}' exists and is active") *>
        describeTable
          .map(s => s == TableStatus.ACTIVE || s == TableStatus.UPDATING)
          .catchSome { case _: ResourceNotFoundException => ZIO.succeed(false) }
          .tap(exists => ZIO.logInfo(s"Lease table ${tableName} exists? ${exists}"))

    // recursion, yeah!
    def awaitTableActive: ZIO[Clock, Throwable, Unit] =
      leaseTableExists
        .flatMap(awaitTableActive.delay(1.seconds).unless(_))
        .unit

    // Optimistically assume the table already exists
    ZIO.logDebug(s"Checking if lease table '${tableName}' exists") *>
      describeTable.flatMap {
        case TableStatus.ACTIVE | TableStatus.UPDATING =>
          ZIO.logDebug(s"Lease table '${tableName}' exists and is active").as(true)
        case TableStatus.CREATING                      =>
          ZIO.logDebug(s"Lease table '${tableName}' has CREATING status") *>
            awaitTableActive.delay(1.seconds).as(true)
        case s @ _                                     =>
          ZIO.fail(new Exception(s"Could not create lease table '${tableName}'. Invalid table status: ${s}"))
      }.catchSome { case _: ResourceNotFoundException =>
        createTable.catchSome {
          // Race condition: another worker is creating the table at the same time as we are, we lose
          case _: ResourceInUseException =>
            ZIO.unit
        } *> awaitTableActive.delay(1.second).as(false)
      }.timeoutFail(new Exception("Timeout creating lease table"))(1.minute)
  }

  override def getLeases(tableName: String): ZStream[Any, Throwable, Lease] =
    client
      .scan(ScanRequest(TableName(tableName)))
      .mapError(_.toThrowable)
      .mapZIO(item => ZIO.fromTry(toLease(item.view.mapValues(_.asEditable).toMap)))

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
  ): ZIO[Any, Either[Throwable, LeaseObsolete.type], Unit] = {
    val request = baseUpdateItemRequestForLease(tableName, lease).copy(
      attributeUpdates = Some(Map(AttributeName("leaseOwner") -> deleteAttributeValueUpdate))
    )

    client
      .updateItem(request)
      .mapError(_.toThrowable)
      .unit
      .catchAll {
        case e: ConditionalCheckFailedException =>
          ZIO.logSpan("Check failed")(ZIO.logErrorCause(Cause.fail(e))) *>
            ZIO.fail(Right(LeaseObsolete))
        case e                                  =>
          ZIO.fail(Left(e))
      }
      .tapError(e => ZIO.logInfo(s"Got error releasing lease ${lease.key}: ${e}"))
  }

  // Returns the updated lease
  override def claimLease(
    tableName: String,
    lease: Lease
  ): ZIO[Clock, Either[Throwable, UnableToClaimLease.type], Unit] = {
    val request = baseUpdateItemRequestForLease(tableName, lease).copy(
      attributeUpdates = Some(
        Map(
          AttributeName("leaseOwner")                   -> putAttributeValueUpdate(lease.owner.get),
          AttributeName("leaseCounter")                 -> putAttributeValueUpdate(lease.counter),
          AttributeName("ownerSwitchesSinceCheckpoint") -> putAttributeValueUpdate(0L) // Just for KCL compatibility
        )
      )
    )

    client
      .updateItem(request)
      .mapError(_.toThrowable)
      .timeoutFail(new TimeoutException(s"Timeout claiming lease"))(timeout)
      // .tapError(e => ZIO.logWarning(s"Got error claiming lease: ${e}"))
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
      TableName(tableName),
      key = DynamoDbItem("leaseKey" -> lease.key),
      expected = Some(Map(AttributeName("leaseCounter") -> expectedAttributeValue(lease.counter - 1)))
    )
  }
// Puts the lease counter to the given lease's counter and expects counter - 1
  override def updateCheckpoint(
    tableName: String,
    lease: Lease
  ): ZIO[Clock, Either[Throwable, LeaseObsolete.type], Unit] = {
    require(lease.checkpoint.isDefined, "Cannot update checkpoint without Lease.checkpoint property set")

    val request = baseUpdateItemRequestForLease(tableName, lease).copy(
      attributeUpdates = Some(
        Map(
          AttributeName("leaseOwner")                   -> lease.owner
            .map(putAttributeValueUpdate(_))
            .getOrElse(deleteAttributeValueUpdate),
          AttributeName("leaseCounter")                 -> putAttributeValueUpdate(lease.counter),
          AttributeName("checkpoint")                   -> lease.checkpoint
            .map(_.fold(_.stringValue, _.sequenceNumber))
            .map(putAttributeValueUpdate)
            .getOrElse(putAttributeValueUpdate(null)),
          AttributeName("checkpointSubSequenceNumber")  -> lease.checkpoint
            .flatMap(_.toOption)
            .map(_.subSequenceNumber)
            .map(putAttributeValueUpdate)
            .getOrElse(putAttributeValueUpdate(0L)),
          AttributeName("ownerSwitchesSinceCheckpoint") -> putAttributeValueUpdate(0L) // Just for KCL compatibility
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
  ): ZIO[Clock, Either[Throwable, LeaseObsolete.type], Unit] = {

    val request = baseUpdateItemRequestForLease(tableName, lease)
      .copy(attributeUpdates = Some(Map(AttributeName("leaseCounter") -> putAttributeValueUpdate(lease.counter))))

    client
      .updateItem(request)
      .mapError(_.toThrowable)
      .timeoutFail(new TimeoutException(s"Timeout renewing lease"))(timeout)
      .tapError(e => ZIO.logWarning(s"Got error updating lease: ${e}"))
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
  ): ZIO[Clock, Either[Throwable, LeaseAlreadyExists.type], Unit] = {
    val request =
      PutItemRequest(
        TableName(tableName),
        toDynamoItem(lease),
        conditionExpression = Some(ConditionExpression("attribute_not_exists(leaseKey)"))
      )

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

  override def deleteTable(
    tableName: String
  ): ZIO[Clock, Throwable, Unit] = {
    val request = DeleteTableRequest(TableName(tableName))
    client
      .deleteTable(request)
      .mapError(_.toThrowable)
      .timeoutFail(new TimeoutException(s"Timeout creating lease"))(timeout)
      .unit
  }

  private def toLease(item: DynamoDbItem): Try[Lease] =
    Try {
      def getValue(key: String): Option[AttributeValue] = item.get(AttributeName(key))
      Lease(
        key = getValue("leaseKey").get.s.toOption.get,
        owner = getValue("leaseOwner").flatMap(_.s.toOption),
        counter = getValue("leaseCounter").flatMap(_.n.toOption).get.toLong,
        checkpoint = getValue("checkpoint")
          .filterNot(_.nul.isDefined)
          .flatMap(_.s.toOption)
          .map(
            toSequenceNumberOrSpecialCheckpoint(
              _,
              getValue("checkpointSubSequenceNumber").flatMap(_.n.toOption).get.toLong
            )
          ),
        parentShardIds = getValue("parentShardIds").map(_.ss.toList.flatten).getOrElse(List.empty)
      )
    }.recoverWith { case e =>
      println(s"Error deserializing lease: ${item} ${e}")
      Failure(e)
    }

  private def toSequenceNumberOrSpecialCheckpoint(
    sequenceNumber: String,
    subsequenceNumber: Long
  ): Either[SpecialCheckpoint, ExtendedSequenceNumber] =
    sequenceNumber match {
      case s if s == SpecialCheckpoint.ShardEnd.stringValue    => Left(SpecialCheckpoint.ShardEnd)
      case s if s == SpecialCheckpoint.TrimHorizon.stringValue => Left(SpecialCheckpoint.TrimHorizon)
      case s if s == SpecialCheckpoint.Latest.stringValue      => Left(SpecialCheckpoint.Latest)
      case s if s == SpecialCheckpoint.AtTimestamp.stringValue => Left(SpecialCheckpoint.AtTimestamp)
      case s                                                   => Right(ExtendedSequenceNumber(s, subsequenceNumber))
    }

  private def toDynamoItem(lease: Lease): DynamoDbItem = {
    import DynamoDbUtil.ImplicitConversions.toAttributeValue
    DynamoDbItem(
      "leaseKey"                    -> lease.key,
      "leaseCounter"                -> lease.counter,
      "checkpoint"                  -> lease.checkpoint.map(_.fold(_.stringValue, _.sequenceNumber)).getOrElse(null),
      "checkpointSubSequenceNumber" -> lease.checkpoint.map(_.fold(_ => 0L, _.subSequenceNumber)).getOrElse(null)
    ) ++ (if (lease.parentShardIds.nonEmpty) DynamoDbItem("parentShardIds" -> lease.parentShardIds)
          else DynamoDbItem.empty) ++
      lease.owner.fold(DynamoDbItem.empty)(owner => DynamoDbItem("leaseOwner" -> owner))
  }
}

object DynamoDbLeaseRepository {
  val defaultTimeout = 10.seconds

  val live: ZLayer[DynamoDb, Nothing, LeaseRepository] = make(defaultTimeout)

  def make(timeout: Duration = defaultTimeout): ZLayer[DynamoDb, Nothing, LeaseRepository] =
    (new DynamoDbLeaseRepository(_, timeout)).toLayer

}
