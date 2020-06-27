package nl.vroste.zio.kinesis.client.zionative

import nl.vroste.zio.kinesis.client.zionative.leasecoordinator.DefaultLeaseCoordinator.Lease
import zio.ZIO
import zio.clock.Clock
import zio.logging.Logging

object LeaseRepository {
  trait Factory {
    def make(applicationName: String): LeaseRepository.Service
  }

  /**
   * Service for storage and retrieval of leases
   */
  trait Service {

    /**
     * Returns whether the table already existed
     */
    def createLeaseTableIfNotExists: ZIO[Clock with Logging, Throwable, Boolean]
    def leaseTableExists: ZIO[Logging, Throwable, Boolean]
    def getLeases: ZIO[Clock, Throwable, List[Lease]]

    /**
     * Removes the leaseOwner property
     *
   * Expects the given lease's counter - 1
     *
   * @param lease
     * @return
     */
    def releaseLease(lease: Lease): ZIO[Logging, Either[Throwable, LeaseObsolete.type], Unit]

// Returns the updated lease
    def claimLease(lease: Lease): ZIO[Logging, Either[Throwable, UnableToClaimLease.type], Unit]

// Puts the lease counter to the given lease's counter and expects counter - 1
    def updateCheckpoint(lease: Lease): ZIO[Logging, Either[Throwable, LeaseObsolete.type], Unit]

    def renewLease(lease: Lease): ZIO[Logging, Either[Throwable, LeaseObsolete.type], Unit]

    def createLease(lease: Lease): ZIO[Logging, Either[Throwable, LeaseAlreadyExists.type], Unit]
  }

  case object LeaseAlreadyExists
  case object UnableToClaimLease
  case object LeaseObsolete
}
