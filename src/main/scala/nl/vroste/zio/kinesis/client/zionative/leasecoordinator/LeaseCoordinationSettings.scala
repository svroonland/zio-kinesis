package nl.vroste.zio.kinesis.client.zionative.leasecoordinator
import zio.duration._

/**
 * Settings affecting lease taking, renewing and refreshing
 *
 * Default values are compatible with KCL defaults (TODO not quite yet)
 *
 * @param expirationTime Time after which a lease is considered expired if not updated in the meantime
 *                       This should be set to several times the lease renewal interval so that leases are
 *                       not considered expired until several renew intervals have been missed, in case of
 *                       transient connection issues.
 * @param refreshAndTakeInterval Interval at which leases are refreshed and possibly new leases taken
 * @param renewInterval Interval at which leases are renewed to prevent them expiring
 * @param maxParallelLeaseAcquisitions Maximum parallel calls to DynamoDB to claim a lease.
 *                                     This is not the maximum number of leases to steal in one iteration
 *                                     of `refreshAndTakeInterval`.
 * @param maxParallelLeaseRenewals Maximum parallel calls to DynamoDB to claim a lease.
 *                                     This is not the maximum number of leases to steal in one iteration
 *                                     of `refreshAndTakeInterval`.
 * @param releaseLeaseTimeout Time after which releasing a lease is silently aborted.
 */
case class LeaseCoordinationSettings(
  expirationTime: Duration = 10.seconds,
  renewInterval: Duration = 3.seconds,
  refreshAndTakeInterval: Duration = 20.seconds,
  maxParallelLeaseAcquisitions: Int = 10,
  maxParallelLeaseRenewals: Int = 10,
  releaseLeaseTimeout: Duration = 10.seconds
) {
  require(renewInterval < expirationTime, "renewInterval must be less than expirationTime")
}
