package nl.vroste.zio.kinesis.client.zionative

import nl.vroste.zio.kinesis.client.zionative.leasecoordinator.DefaultLeaseCoordinator.Lease
import zio.ZIO
import zio.clock.Clock
import zio.logging.Logging

object LeaseRepository {

  /**
   * Service for storage and retrieval of leases
   * TODO cleanup
   */
  trait Service {

    /**
     * Returns whether the table already existed
     */
    def createLeaseTableIfNotExists(tableName: String): ZIO[Clock with Logging, Throwable, Boolean]
//    def leaseTableExists(tableName: String): ZIO[Logging, Throwable, Boolean]
    def getLeases(tableName: String): ZIO[Clock, Throwable, List[Lease]]

    /**
     * Removes the leaseOwner property
     *
   * Expects the given lease's counter - 1
     *
   * @param lease
     * @return
     */
    def releaseLease(tableName: String, lease: Lease): ZIO[Logging, Either[Throwable, LeaseObsolete.type], Unit]

// Returns the updated lease
    def claimLease(tableName: String, lease: Lease): ZIO[Logging, Either[Throwable, UnableToClaimLease.type], Unit]

// Puts the lease counter to the given lease's counter and expects counter - 1
    def updateCheckpoint(tableName: String, lease: Lease): ZIO[Logging, Either[Throwable, LeaseObsolete.type], Unit]

    def renewLease(tableName: String, lease: Lease): ZIO[Logging, Either[Throwable, LeaseObsolete.type], Unit]

    def createLease(tableName: String, lease: Lease): ZIO[Logging, Either[Throwable, LeaseAlreadyExists.type], Unit]
  }

  case object LeaseAlreadyExists
  case object UnableToClaimLease
  case object LeaseObsolete
}
