package nl.vroste.zio.kinesis.client.zionative.metrics

import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient

import nl.vroste.zio.kinesis.client.Util.asZIO
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum

import scala.jdk.CollectionConverters._
import zio.Task
import zio.Queue
import nl.vroste.zio.kinesis.client.zionative.DiagnosticEvent
import zio.stream.ZStream
import nl.vroste.zio.kinesis.client.zionative.DiagnosticEvent.PollComplete
import zio.stream.ZTransducer
import zio.Schedule
import zio.duration._
import zio.ZIO
import nl.vroste.zio.kinesis.client.zionative.DiagnosticEvent.SubscribeToShardEvent
import nl.vroste.zio.kinesis.client.zionative.DiagnosticEvent.LeaseAcquired
import nl.vroste.zio.kinesis.client.zionative.DiagnosticEvent.ShardLeaseLost
import nl.vroste.zio.kinesis.client.zionative.DiagnosticEvent.LeaseRenewed
import nl.vroste.zio.kinesis.client.zionative.DiagnosticEvent.LeaseReleased
import nl.vroste.zio.kinesis.client.zionative.DiagnosticEvent.Checkpoint
import nl.vroste.zio.kinesis.client.zionative.DiagnosticEvent.WorkerJoined
import nl.vroste.zio.kinesis.client.zionative.DiagnosticEvent.WorkerLeft
import nl.vroste.zio.kinesis.client.zionative.MetricsPublisher
import software.amazon.awssdk.services.cloudwatch.model.Dimension
import scala.jdk.CollectionConverters._
import java.time.Instant
import zio.ZManaged
import zio.UIO
import zio.ZLayer
import zio.clock.Clock
import zio.Has
import zio.logging.{ log, Logging }
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit

class CloudWatchMetricsPublisher(client: CloudWatchAsyncClient, eventQueue: Queue[DiagnosticEvent], namespace: String)
    extends MetricsPublisher.Service {
  val flushInterval: Duration = 20.seconds
  val maxBatchSize            = 20 // SDK limit

  def processEvent(e: DiagnosticEvent): UIO[Unit] = eventQueue.offer(e).unit

  private def dimension(name: String, value: String) = Dimension.builder().name(name).value(value).build()

  private def metric(
    name: String,
    value: Double,
    timestamp: Instant,
    dimensions: Seq[(String, String)],
    unit: StandardUnit
  ) =
    MetricDatum
      .builder()
      .metricName(name)
      .dimensions(dimensions.map(Function.tupled(dimension)).asJava)
      .value(value)
      .timestamp(timestamp)
      .unit(unit)
      .build()

  def toMetrics(e: DiagnosticEvent, timestamp: Instant): List[MetricDatum] =
    e match {
      case PollComplete(shardId, nrRecords, behindLatest, duration) =>
        shardFetchMetrics(shardId, nrRecords, behindLatest, duration, timestamp)
      case SubscribeToShardEvent(shardId, nrRecords, behindLatest)  =>
        shardFetchMetrics(shardId, nrRecords, behindLatest, 0.millis, timestamp) // TODO what to do with duration
      case LeaseAcquired(shardId, checkpoint)                       => List.empty
      case ShardLeaseLost(shardId)                                  => List.empty
      case LeaseRenewed(shardId)                                    => List.empty
      case LeaseReleased(shardId)                                   => List.empty
      case Checkpoint(shardId, checkpoint)                          => List.empty
      case WorkerJoined(workerId)                                   => List.empty
      case WorkerLeft(workerId)                                     => List.empty
    }

  private def shardFetchMetrics(
    shardId: String,
    nrRecords: Int,
    behindLatest: Duration,
    duration: Duration,
    timestamp: Instant
  ): List[MetricDatum] =
    List(
      metric(
        "RecordsProcessed",
        nrRecords,
        timestamp,
        Seq("ShardId" -> shardId, "Operation" -> "ProcessTask"),
        StandardUnit.COUNT
      ), // TODO fetched != processed
      metric(
        "Time",
        duration.toMillis,
        timestamp,
        Seq("ShardId" -> shardId, "Operation" -> "ProcessTask"),
        StandardUnit.MILLISECONDS
      ),
      metric(
        "MillisBehindLatest",
        behindLatest.toMillis,
        timestamp,
        Seq("ShardId" -> shardId, "Operation" -> "ProcessTask"),
        StandardUnit.MILLISECONDS
      )
    )

  val now = zio.clock.currentDateTime.map(_.toInstant())

  // TODO make sure queue is closed
  val processQueue =
    ZStream
      .fromQueue(eventQueue)
      .mapM(e => now.map((e, _)))
      .mapConcat(Function.tupled(toMetrics))
      .aggregateAsyncWithin(ZTransducer.collectAllN(maxBatchSize), Schedule.fixed(flushInterval))
      .mapM(putMetricData) // TODO should probably retry
      .runDrain

  private def putMetricData(metricData: Seq[MetricDatum]): ZIO[Logging, Throwable, Unit] = {
    val request = PutMetricDataRequest
      .builder()
      .metricData(metricData.asJava)
      .namespace(namespace)
      .build()

    // log.info(s"Putting metrics: ${request}") *>
    asZIO(client.putMetricData(request)).unit
  }
}
