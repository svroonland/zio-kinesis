package nl.vroste.zio.kinesis.client.producer
import java.time.Instant

import nl.vroste.zio.kinesis.client.producer.ProducerMetrics.{
  emptyAttempts,
  emptyLatency,
  emptyPayloadSizes,
  emptyRecordSizes
}
import org.HdrHistogram.{ AbstractHistogram, Histogram, IntCountsHistogram }
import zio.duration.Duration

private[client] final case class CurrentMetrics(
  start: Instant,
  published: IntCountsHistogram,   // Tracks number of attempts
  nrFailed: Long,
  latency: Histogram,
  shardPredictionErrors: Long,
  payloadSize: IntCountsHistogram, // PutRecords call histogram
  recordSize: IntCountsHistogram   // PutRecords call histogram
) {
  val nrPublished = published.getTotalCount

  def addSuccess(attempts: Int, latency1: Duration): CurrentMetrics =
    addSuccesses(Seq(attempts), Seq(latency1))

  def addFailures(nr: Int): CurrentMetrics =
    copy(nrFailed = nrFailed + nr)

  def addSuccesses(publishedWithAttempts: Seq[Int], latencies: Seq[Duration]): CurrentMetrics =
    copy(
      published = addToHistogram(published, publishedWithAttempts.map(_.toLong)),
      latency = addToHistogram(latency, latencies.map(_.toMillis max 0))
    )

  def addShardPredictionErrors(nr: Long): CurrentMetrics = copy(shardPredictionErrors = shardPredictionErrors + nr)

  def addPayloadSize(size: Int): CurrentMetrics =
    copy(payloadSize = addToHistogram(payloadSize, Seq(size.toLong)))

  def addRecordSizes(sizes: Seq[Int]): CurrentMetrics =
    copy(recordSize = addToHistogram(payloadSize, sizes.map(_.toLong)))

  private def addToHistogram[T <: AbstractHistogram](hist: T, values: Seq[Long]): T = {
    val newHist = hist.copy().asInstanceOf[T]
    values.foreach(newHist.recordValue)
    newHist
  }
}

private[client] object CurrentMetrics {
  def empty(now: Instant): CurrentMetrics =
    CurrentMetrics(
      start = now,
      published = emptyAttempts,
      nrFailed = 0,
      latency = emptyLatency,
      shardPredictionErrors = 0,
      payloadSize = emptyPayloadSizes,
      recordSize = emptyRecordSizes
    )
}
