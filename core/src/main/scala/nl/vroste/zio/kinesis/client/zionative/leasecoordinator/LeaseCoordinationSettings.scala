package nl.vroste.zio.kinesis.client.zionative.leasecoordinator

import nl.vroste.zio.kinesis.client.Util
import zio.{ Schedule, _ }

/**
 * Settings affecting lease taking, renewing and refreshing
 *
 * Default values are compatible with KCL defaults (TODO not quite yet)
 *
 * @param refreshAndTakeInterval
 *   Interval at which leases are refreshed and possibly new leases taken
 * @param renewInterval
 *   Interval at which leases are renewed to prevent them expiring
 * @param maxParallelLeaseAcquisitions
 *   Maximum parallel calls to DynamoDB to claim a lease. This is not the maximum number of leases to steal in one
 *   iteration of `refreshAndTakeInterval`.
 * @param maxParallelLeaseRenewals
 *   Maximum parallel calls to DynamoDB to claim a lease. This is not the maximum number of leases to steal in one
 *   iteration of `refreshAndTakeInterval`.
 * @param releaseLeaseTimeout
 *   Time after which releasing a lease is silently aborted.
 * @param renewRetrySchedule
 *   Schedule that controls retries when exceptions occur when renewing a lease. The lease is released (internally only)
 *   when the schedule fails.
 * @param shardRefreshInterval
 *   Interval at which the stream's shards are refreshed
 */
final case class LeaseCoordinationSettings(
  renewInterval: Duration = 3.seconds,
  refreshAndTakeInterval: Duration = 20.seconds,
  maxParallelLeaseAcquisitions: Int = 10,
  maxParallelLeaseRenewals: Int = 10,
  releaseLeaseTimeout: Duration = 10.seconds,
  renewRetrySchedule: Schedule[Any, Throwable, Any] =
    Util.exponentialBackoff(3.second, 30.seconds, maxRecurs = Some(3)),
  shardRefreshInterval: Duration = 30.seconds
)
