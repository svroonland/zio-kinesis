package nl.vroste.zio.kinesis.client.producer

import org.HdrHistogram.{ AbstractHistogram, Histogram, IntCountsHistogram }

import zio._

/**
 * Kinesis record producing metrics
 *
 * Can be combined with the `+` operator with other `ProducerMetrics` to get statistically sound total metrics
 *
 * @param interval
 *   Interval over which metrics were collected
 * @param attempts
 *   Histogram of number of attempts needed to successfully publish
 * @param nrFailures
 *   Number of failed record publish attempts
 * @param latency
 *   Histogram of latency between enqueuing and successful publishing
 */
final case class ProducerMetrics(
  interval: Duration,
  attempts: IntCountsHistogram,
  nrFailures: Long,
  latency: AbstractHistogram,
  shardPredictionErrors: Long,
  payloadSize: IntCountsHistogram,
  recordSize: IntCountsHistogram
) {
  val nrRecordsPublished: Long = attempts.getTotalCount
  val nrPutRecordCalls: Long   = payloadSize.getTotalCount

  /**
   * Of all publish attempts, how many were successful
   *
   * Between 0 and 1
   */
  val successRate: Double =
    if (nrRecordsPublished + nrFailures > 0) nrRecordsPublished * 1.0 / (nrRecordsPublished + nrFailures)
    else 1

  val throughput: Option[Double] =
    if (interval.toMillis > 0) Some(nrRecordsPublished * 1000.0 / interval.toMillis) else None

  val meanNrPutRecordCalls: Double = if (interval > Duration.Zero) nrPutRecordCalls * 1000.0 / interval.toMillis else 0

  override def toString: String =
    Seq(
      ("interval", interval.getSeconds, "s"),
      ("total records published", nrRecordsPublished, ""),
      ("throughput (records)", throughput.getOrElse(0), "records/s"),
      ("throughput (bytes)", (payloadSize.getMean * meanNrPutRecordCalls).toInt, "bytes/s"),
      ("success rate", "%.02f".format(successRate * 100), "%"),
      ("failed attempts", nrFailures, ""),
      ("shard prediction errors", shardPredictionErrors, ""),
      ("mean latency", latency.getMean.toInt, "ms"),
      ("95% latency", latency.getValueAtPercentile(95).toInt, "ms"),
      ("min latency", latency.getMinValue.toInt, "ms"),
      ("2nd attempts", attempts.getCountAtValue(2), ""),
      ("max attempts", attempts.getMaxValue, ""),
      ("mean payload size", payloadSize.getMean.toInt, "bytes"),
      ("mean record size", recordSize.getMean.toInt, "bytes"),
      ("nr PutRecords calls", nrPutRecordCalls, ""),
      ("mean nr PutRecords calls", meanNrPutRecordCalls, "calls/s")
    ).map { case (name, value, unit) => s"${name}=${value}${if (unit.isEmpty) "" else " " + unit}" }.mkString(", ")

  /**
   * Combine with other metrics to get a statically sound combined metrics
   */
  def +(that: ProducerMetrics): ProducerMetrics =
    ProducerMetrics(
      interval = interval + that.interval,
      attempts = mergeHistograms(attempts, that.attempts),
      nrFailures = nrFailures + that.nrFailures,
      latency = mergeHistograms(latency, that.latency),
      shardPredictionErrors = shardPredictionErrors + that.shardPredictionErrors,
      payloadSize = mergeHistograms(payloadSize, that.payloadSize),
      recordSize = mergeHistograms(recordSize, that.recordSize)
    )

  private def mergeHistograms[T <: AbstractHistogram](h1: T, h2: T): T = {
    val newHist = h1.copy()
    newHist.add(h2)
    newHist.asInstanceOf[T]
  }
}

object ProducerMetrics {
  private[client] def emptyAttempts     = new IntCountsHistogram(1, 20, 2)
  private[client] def emptyLatency      = new Histogram(1, 120000, 3)
  private[client] def emptyPayloadSizes = new IntCountsHistogram(1, ProducerLive.maxPayloadSizePerRequest.toLong, 5)
  private[client] def emptyRecordSizes  = new IntCountsHistogram(1, ProducerLive.maxPayloadSizePerRecord.toLong, 4)

  val empty = ProducerMetrics(
    interval = 0.millis,
    attempts = emptyAttempts,
    nrFailures = 0,
    latency = emptyLatency,
    shardPredictionErrors = 0,
    payloadSize = emptyPayloadSizes,
    recordSize = emptyRecordSizes
  )
}
