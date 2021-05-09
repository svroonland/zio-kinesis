package nl.vroste.zio.kinesis.client.producer
import nl.vroste.zio.kinesis.client.producer.ProducerMetrics.{
  emptyAttempts,
  emptyLatency,
  emptyPayloadSizes,
  emptyRecordSizes
}
import org.HdrHistogram.{ Histogram, IntCountsHistogram }
import zio.Chunk
import zio.duration.Duration

import java.time.Instant

private[client] final case class CurrentMetrics(
  start: Instant,
  nrFailed: Long,
  shardPredictionErrors: Long,
  published: Chunk[Int], // Tracks number of attempts
  payloadSizes: Chunk[Int],
  recordSizes: Chunk[Int],
  latencies: Chunk[Long]
) {
  val nrPublished = published.size

  def addSuccess(attempts: Int, latency1: Duration): CurrentMetrics =
    copy(published = published :+ attempts, latencies = latencies :+ latency1.toMillis)

  def addFailures(nr: Int): CurrentMetrics =
    copy(nrFailed = nrFailed + nr)

  def addSuccesses(publishedWithAttempts: Chunk[Int], latenciesNew: Chunk[Duration]): CurrentMetrics =
    copy(published = published ++ publishedWithAttempts, latencies = latencies ++ latenciesNew.map(_.toMillis))

  def addShardPredictionErrors(nr: Long): CurrentMetrics = copy(shardPredictionErrors = shardPredictionErrors + nr)

  def addPayloadSize(size: Int): CurrentMetrics =
    copy(payloadSizes = payloadSizes :+ size)

  def addRecordSizes(sizes: Chunk[Int]): CurrentMetrics =
    copy(recordSizes = recordSizes ++ sizes)

  def publishedHist: IntCountsHistogram = {
    val hist = emptyAttempts
    published.foreach(i => hist.recordValue(i.toLong))
    hist
  }

  def latencyHist: Histogram = {
    val hist = emptyLatency
    latencies.foreach(hist.recordValue)
    hist
  }

  def payloadSizeHist: IntCountsHistogram = {
    val hist = emptyPayloadSizes
    payloadSizes.foreach(i => hist.recordValue(i.toLong))
    hist
  }

  def recordSizeHist: IntCountsHistogram = {
    val hist = emptyRecordSizes
    recordSizes.foreach(i => hist.recordValue(i.toLong))
    hist
  }

}

private[client] object CurrentMetrics {
  def empty(now: Instant): CurrentMetrics =
    CurrentMetrics(
      start = now,
      nrFailed = 0,
      shardPredictionErrors = 0,
      published = Chunk.empty,
      payloadSizes = Chunk.empty,
      recordSizes = Chunk.empty,
      latencies = Chunk.empty
    )
}
