package nl.vroste.zio.kinesis.client.producer

import org.HdrHistogram.{ AbstractHistogram, Histogram, IntCountsHistogram }
import zio.duration._

/**
 * Kinesis record producing metrics
 *
 * Can be combined with the `+` operator with other `ProducerMetrics` to get statistically sound total metrics
 *
 * @param interval Interval over which metrics were collected
 * @param attempts Histogram of number of attempts needed to successfully publish
 * @param nrFailures Number of failed record publish attempts
 * @param latency Histogram of latency between enqueuing and successful publishing
 */
final case class ProducerMetrics(
  interval: Duration,
  attempts: IntCountsHistogram,
  nrFailures: Long,
  latency: AbstractHistogram,
  shardPredictionErrors: Long
) {
  val nrRecordsPublished: Long = attempts.getTotalCount

  /**
   * Of all publish attempts, how many were successful
   *
   * Between 0 and 1
   */
  val successRate: Double =
    if (nrRecordsPublished + nrFailures > 0) (nrRecordsPublished * 1.0 / (nrRecordsPublished + nrFailures))
    else 1

  val throughput: Option[Double] =
    if (interval.toMillis > 0) Some(nrRecordsPublished * 1000.0 / interval.toMillis) else None

  override def toString: String =
    s"{" +
      s"interval=${interval.getSeconds}s, " +
      s"total records published=${nrRecordsPublished}, " +
      s"throughput=${throughput.map(_ + "/s").getOrElse("unknown")}, " +
      s"success rate=${successRate * 100}%, " +
      s"failed attempts=${nrFailures}, " +
      s"shard prediction errors=${shardPredictionErrors}, " +
      s"mean latency=${latency.getMean.toInt}ms, " +
      s"95% latency=${latency.getValueAtPercentile(95).toInt}.ms, " +
      s"min latency=${latency.getMinValue.toInt}ms, " +
      s"2nd attempts=${attempts.getCountAtValue(2)}, " +
      s"max attempts=${attempts.getMaxValue}" +
      s"}"

  /**
   * Combine with other metrics to get a statically sound combined metrics
   */
  def +(that: ProducerMetrics): ProducerMetrics =
    ProducerMetrics(
      interval = interval + that.interval,
      attempts = { val newAttempts = attempts.copy(); newAttempts.add(that.attempts); newAttempts },
      nrFailures = nrFailures + that.nrFailures,
      latency = { val newLatency = latency.copy(); newLatency.add(that.latency); newLatency },
      shardPredictionErrors = shardPredictionErrors + that.shardPredictionErrors
    )
}

object ProducerMetrics {
  private[client] val emptyAttempts = new IntCountsHistogram(1, 20, 2)
  private[client] val emptyLatency  = new Histogram(1, 120000, 3)

  val empty = ProducerMetrics(
    interval = 0.millis,
    attempts = emptyAttempts,
    nrFailures = 0,
    latency = emptyLatency,
    shardPredictionErrors = 0
  )
}
