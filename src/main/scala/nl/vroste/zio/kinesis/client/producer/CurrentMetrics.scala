package nl.vroste.zio.kinesis.client.producer
import java.time.Instant

import nl.vroste.zio.kinesis.client.producer.ProducerMetrics.{ emptyAttempts, emptyLatency }
import org.HdrHistogram.{ Histogram, IntCountsHistogram }
import zio.duration.Duration

private[client] final case class CurrentMetrics(
  start: Instant,
  published: IntCountsHistogram, // Tracks number of attempts
  nrFailed: Long,
  latency: Histogram,
  shardPredictionErrors: Long
) {
  val nrPublished = published.getTotalCount

  def addSuccess(attempts: Int, latency1: Duration): CurrentMetrics = {
    val newLatency = latency.copy()
    newLatency.recordValue(latency1.toMillis max 0)

    val newPublished = published.copy()
    newPublished.recordValue(attempts.toLong)

    copy(published = newPublished, latency = newLatency)
  }

  def addFailures(nr: Int): CurrentMetrics =
    copy(nrFailed = nrFailed + nr)

  def addSuccesses(publishedWithAttempts: Seq[Int], latencies: Seq[Duration]): CurrentMetrics = {
    val newLatency = latency.copy()
    latencies.map(_.toMillis max 0).foreach(newLatency.recordValue)

    val newPublished = published.copy()
    publishedWithAttempts.map(_.toLong).foreach(newPublished.recordValue)

    copy(published = newPublished, latency = newLatency)
  }

  def addShardPredictionErrors(nr: Long): CurrentMetrics = copy(shardPredictionErrors = shardPredictionErrors + nr)
}

private[client] object CurrentMetrics {
  def empty(now: Instant): CurrentMetrics =
    CurrentMetrics(
      start = now,
      published = emptyAttempts,
      nrFailed = 0,
      latency = emptyLatency,
      shardPredictionErrors = 0
    )
}
