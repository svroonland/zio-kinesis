package nl.vroste.zio.kinesis.client.zionative

import zio.ZIO
import zio.clock.Clock
import zio.logging.Logging

object LeaseRepository {
  case class Lease(
    key: String,
    owner: Option[String],
    counter: Long,
    checkpoint: Option[ExtendedSequenceNumber],
    parentShardIds: Seq[String]
  ) {
    def increaseCounter: Lease = copy(counter = counter + 1)

    def claim(owner: String): Lease =
      copy(owner = Some(owner), counter = counter + 1)
  }

  /**
   * Service for storage and retrieval of leases
   * TODO cleanup
   */
  trait Service {

    /**
     * Returns whether the table already existed
     */
    def createLeaseTableIfNotExists(tableName: String): ZIO[Clock with Logging, Throwable, Boolean]
    def getLeases(tableName: String): ZIO[Clock, Throwable, List[Lease]]

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
    ): ZIO[Logging with Clock, Either[Throwable, LeaseObsolete.type], Unit]

// Returns the updated lease
    def claimLease(
      tableName: String,
      lease: Lease
    ): ZIO[Logging with Clock, Either[Throwable, UnableToClaimLease.type], Unit]

// Puts the lease counter to the given lease's counter and expects counter - 1
    def updateCheckpoint(
      tableName: String,
      lease: Lease
    ): ZIO[Logging with Clock, Either[Throwable, LeaseObsolete.type], Unit]

    def renewLease(
      tableName: String,
      lease: Lease
    ): ZIO[Logging with Clock, Either[Throwable, LeaseObsolete.type], Unit]

    def createLease(
      tableName: String,
      lease: Lease
    ): ZIO[Logging with Clock, Either[Throwable, LeaseAlreadyExists.type], Unit]
  }

  case object LeaseAlreadyExists
  case object UnableToClaimLease
  case object LeaseObsolete
}
