package nl.vroste.zio.kinesis.client.zionative

import zio.{ Clock, ZIO }
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

  /**
   * Service for storage and retrieval of leases
   * TODO cleanup
   */
  trait Service {

    /**
     * Returns whether the table already existed
     */
    def createLeaseTableIfNotExists(tableName: String): ZIO[Clock, Throwable, Boolean]

    def deleteTable(tableName: String): ZIO[Clock, Throwable, Unit]

    def getLeases(tableName: String): ZStream[Clock, Throwable, Lease]

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
    ): ZIO[Clock, Either[Throwable, LeaseObsolete.type], Unit]

// Returns the updated lease
    def claimLease(
      tableName: String,
      lease: Lease
    ): ZIO[Clock, Either[Throwable, UnableToClaimLease.type], Unit]

// Puts the lease counter to the given lease's counter and expects counter - 1
    def updateCheckpoint(
      tableName: String,
      lease: Lease
    ): ZIO[Clock, Either[Throwable, LeaseObsolete.type], Unit]

    def renewLease(
      tableName: String,
      lease: Lease
    ): ZIO[Clock, Either[Throwable, LeaseObsolete.type], Unit]

    def createLease(
      tableName: String,
      lease: Lease
    ): ZIO[Clock, Either[Throwable, LeaseAlreadyExists.type], Unit]
  }

  case object LeaseAlreadyExists
  case object UnableToClaimLease
  case object LeaseObsolete
}
