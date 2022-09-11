package nl.vroste.zio.kinesis.client.zionative

import nl.vroste.zio.kinesis.client.zionative.LeaseRepository.{
  Lease,
  LeaseAlreadyExists,
  LeaseObsolete,
  UnableToClaimLease
}
import zio.ZIO
import zio.stream.ZStream

sealed trait SpecialCheckpoint {
  val stringValue: String
}

object SpecialCheckpoint {
  case object ShardEnd extends SpecialCheckpoint {
    val stringValue = "SHARD_END"
  }

  case object TrimHorizon extends SpecialCheckpoint {
    val stringValue = "TRIM_HORIZON"
  }

  case object Latest extends SpecialCheckpoint {
    val stringValue = "LATEST"
  }

  case object AtTimestamp extends SpecialCheckpoint {
    val stringValue = "AT_TIMESTAMP"
  }
}

/**
 * Service for storage and retrieval of leases TODO cleanup
 */
trait LeaseRepository {

  /**
   * Returns whether the table already existed
   */
  def createLeaseTableIfNotExists(tableName: String): ZIO[Any, Throwable, Boolean]

  def deleteTable(tableName: String): ZIO[Any, Throwable, Unit]

  def getLeases(tableName: String): ZStream[Any, Throwable, Lease]

  /**
   * Removes the leaseOwner property
   *
   * Expects the given lease's counter - 1
   *
   * @param lease
   * @return
   */
  def releaseLease(
    tableName: String,
    lease: Lease
  ): ZIO[Any, Either[Throwable, LeaseObsolete.type], Unit]

  // Returns the updated lease
  def claimLease(
    tableName: String,
    lease: Lease
  ): ZIO[Any, Either[Throwable, UnableToClaimLease.type], Unit]

  // Puts the lease counter to the given lease's counter and expects counter - 1
  def updateCheckpoint(
    tableName: String,
    lease: Lease
  ): ZIO[Any, Either[Throwable, LeaseObsolete.type], Unit]

  def renewLease(
    tableName: String,
    lease: Lease
  ): ZIO[Any, Either[Throwable, LeaseObsolete.type], Unit]

  def createLease(
    tableName: String,
    lease: Lease
  ): ZIO[Any, Either[Throwable, LeaseAlreadyExists.type], Unit]
}

object LeaseRepository {
  final case class Lease(
    key: String,
    owner: Option[String],
    counter: Long,
    checkpoint: Option[Either[SpecialCheckpoint, ExtendedSequenceNumber]],
    parentShardIds: Seq[String]
  ) {
    def increaseCounter: Lease = copy(counter = counter + 1)

    def claim(owner: String): Lease =
      copy(owner = Some(owner), counter = counter + 1)

    def release: Lease =
      copy(owner = None)
  }

  case object LeaseAlreadyExists
  case object UnableToClaimLease
  case object LeaseObsolete
}
