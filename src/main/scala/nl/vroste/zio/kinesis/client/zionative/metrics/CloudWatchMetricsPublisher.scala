package nl.vroste.zio.kinesis.client.zionative.metrics

import java.time.Instant

import nl.vroste.zio.kinesis.client.Util
import nl.vroste.zio.kinesis.client.Util.asZIO
import nl.vroste.zio.kinesis.client.zionative.DiagnosticEvent
import nl.vroste.zio.kinesis.client.zionative.DiagnosticEvent._
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.cloudwatch.model.{ Dimension, MetricDatum, PutMetricDataRequest, StandardUnit }
import zio._
import zio.clock.Clock
import zio.duration._
import zio.logging.{ log, Logging }
import zio.stream.{ ZStream, ZTransducer }

import scala.jdk.CollectionConverters._

/**
 * Configuration for CloudWatch metrics publishing
 *
  * @param maxFlushInterval Collected metrics will be uploaded to CloudWatch at most this interval
 * @param maxBatchSize Collected metrics will be uploaded to CloudWatch at most this number of metrics. Must be <= 20 (AWS SDK limit)
 * @param periodicMetricInterval Periodic metrics (nr leases / nr workers) are collected at this interval
 * @param retrySchedule Transient upload failures are retried according to this schedule
 * @param maxParallelUploads The maximum number of in-flight requests with batches of metric data to CloudWatch
 */
case class CloudWatchMetricsPublisherConfig(
  maxFlushInterval: Duration = 20.seconds,
  maxBatchSize: Int = 20,
  periodicMetricInterval: Duration = 30.seconds,
  retrySchedule: Schedule[Clock, Any, (Duration, Int)] = Util.exponentialBackoff(1.second, 1.minute),
  maxParallelUploads: Int = 3
) {
  require(maxBatchSize <= 20, "maxBatchSize must be <= 20 (AWS SDK limit)")
}

/**
 * Publishes KCL compatible metrics to CloudWatch
 */
private class CloudWatchMetricsPublisher(
  client: CloudWatchAsyncClient,
  eventQueue: Queue[DiagnosticEvent],
  periodicMetricsQueue: Queue[MetricDatum],
  namespace: String,
  workerId: String,
  heldLeases: Ref[Set[String]],
  workers: Ref[Set[String]],
  config: CloudWatchMetricsPublisherConfig
) extends CloudWatchMetricsPublisher.Service {
  import CloudWatchMetricsPublisher._

  def processEvent(e: DiagnosticEvent): UIO[Unit] = eventQueue.offer(e).unit

  private def toMetrics(e: DiagnosticEvent, timestamp: Instant): List[MetricDatum] =
    e match {
      case PollComplete(shardId, nrRecords, behindLatest, duration) =>
        shardFetchMetrics(shardId, nrRecords, behindLatest, duration, timestamp)
      case SubscribeToShardEvent(shardId, nrRecords, behindLatest)  =>
        shardFetchMetrics(shardId, nrRecords, behindLatest, 0.millis, timestamp) // TODO what to do with duration
      case LeaseAcquired(shardId @ _, checkpoint @ _)               => List.empty // Processed in periodic metrics
      case ShardLeaseLost(shardId @ _)                              =>
        List(
          metric(
            "LostLeases",
            1,
            timestamp,
            Seq("WorkerIdentifier" -> workerId, "Operation" -> "RenewAllLeases"),
            StandardUnit.COUNT
          )
        )
      case LeaseRenewed(shardId @ _, duration)                      =>
        List(
          metric(
            "RenewLease.Time",
            duration.toMillis.toDouble,
            timestamp,
            Seq("WorkerIdentifier" -> workerId, "Operation" -> "RenewAllLeases"),
            StandardUnit.MILLISECONDS
          ),
          metric(
            "RenewLease.Success",
            1,
            timestamp,
            Seq("WorkerIdentifier" -> workerId, "Operation" -> "RenewAllLeases"),
            StandardUnit.COUNT
          )
        )
      case LeaseReleased(shardId @ _)                               => List.empty // Processed in periodic metrics
      case Checkpoint(shardId @ _, checkpoint @ _)                  => List.empty
      case WorkerJoined(workerId @ _)                               => List.empty // Processed in periodic metrics
      case WorkerLeft(workerId @ _)                                 => List.empty // Processed in periodic metrics
      // TODO LeaseCreated (for new leases)
      // TODO lease taken
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
        nrRecords.toDouble,
        timestamp,
        Seq("ShardId" -> shardId, "Operation" -> "ProcessTask"),
        StandardUnit.COUNT
      ), // TODO fetched != quite processed
      metric(
        "Time",
        duration.toMillis.toDouble,
        timestamp,
        Seq("ShardId" -> shardId, "Operation" -> "ProcessTask"),
        StandardUnit.MILLISECONDS
      ),
      metric(
        "MillisBehindLatest",
        behindLatest.toMillis.toDouble,
        timestamp,
        Seq("ShardId" -> shardId, "Operation" -> "ProcessTask"),
        StandardUnit.MILLISECONDS
      )
    )

  val now = zio.clock.currentDateTime.map(_.toInstant())

  def collectPeriodicMetrics(event: DiagnosticEvent): UIO[Unit] =
    event match {
      case PollComplete(_, _, _, _)       => UIO.unit
      case SubscribeToShardEvent(_, _, _) => UIO.unit

      case LeaseAcquired(shardId, _) => heldLeases.update(_ + shardId)
      case ShardLeaseLost(shardId)   => heldLeases.update(_ - shardId)
      case LeaseRenewed(_, _)        => UIO.unit
      case LeaseReleased(shardId)    => heldLeases.update(_ - shardId)
      case Checkpoint(_, _)          => UIO.unit
      case WorkerJoined(workerId)    => workers.update(_ + workerId)
      case WorkerLeft(workerId)      => workers.update(_ - workerId)
    }

  val processQueue =
    (ZStream
      .fromQueue(eventQueue)
      .mapM(event => now.map((event, _)))
      .tap { case (e, _) => collectPeriodicMetrics(e) }
      .mapConcat(Function.tupled(toMetrics)) merge ZStream.fromQueue(periodicMetricsQueue))
      .aggregateAsyncWithin(
        ZTransducer.collectAllN(config.maxBatchSize.toLong),
        Schedule.fixed(config.maxFlushInterval)
      )
      .mapMPar(config.maxParallelUploads) { metrics =>
        putMetricData(metrics)
          .tapError(e => log.warn(s"Failed to upload metrics, will retry: ${e}"))
          .retry(config.retrySchedule)
          .orDie // orDie because schedule has Any as error type?
      }
      .runDrain
      .tapCause(e => log.error("Metrics uploading has stopped with error", e))

  val generatePeriodicMetrics =
    (for {
      now            <- now
      nrWorkers      <- workers.get.map(_.size)
      nrLeases       <- heldLeases.get.map(_.size)
      _               = println(s"Worker ${workerId} has ${nrLeases} leases")
      nrWorkersMetric = metric(
                          "NumWorkers",
                          nrWorkers.toDouble,
                          now,
                          Seq("Operation" -> "TakeLeases", "WorkerIdentifier" -> workerId),
                          StandardUnit.COUNT
                        )
      nrLeasesMetric  = metric(
                         "TotalLeases",
                         nrLeases.toDouble,
                         now,
                         Seq("Operation" -> "TakeLeases", "WorkerIdentifier" -> workerId),
                         StandardUnit.COUNT
                       )
      nrLeasesMetric2 = metric(
                          "CurrentLeases",
                          nrLeases.toDouble,
                          now,
                          Seq("Operation" -> "RenewAllLeases", "WorkerIdentifier" -> workerId),
                          StandardUnit.COUNT
                        )
      _              <- periodicMetricsQueue.offerAll(List(nrWorkersMetric, nrLeasesMetric, nrLeasesMetric2))
    } yield ()).repeat(Schedule.fixed(config.periodicMetricInterval))

  private def putMetricData(metricData: Seq[MetricDatum]): ZIO[Logging, Throwable, Unit] = {
    val request = PutMetricDataRequest
      .builder()
      .metricData(metricData.asJava)
      .namespace(namespace)
      .build()

    asZIO(client.putMetricData(request)).unit
  }
}

object CloudWatchMetricsPublisher {
  trait Service {
    def processEvent(e: DiagnosticEvent): UIO[Unit]
  }

  def make(applicationName: String, workerId: String): ZManaged[Clock with Logging with Has[
    CloudWatchAsyncClient
  ] with Has[CloudWatchMetricsPublisherConfig], Nothing, Service] =
    for {
      client  <- ZManaged.service[CloudWatchAsyncClient]
      config  <- ZManaged.service[CloudWatchMetricsPublisherConfig]
      q       <- Queue.bounded[DiagnosticEvent](1000).toManaged_
      q2      <- Queue.bounded[MetricDatum](1000).toManaged_
      leases  <- Ref.make[Set[String]](Set.empty).toManaged_
      workers <- Ref.make[Set[String]](Set.empty).toManaged_
      c        = new CloudWatchMetricsPublisher(client, q, q2, applicationName, workerId, leases, workers, config)
      _       <- c.processQueue.forkManaged
      _       <- c.generatePeriodicMetrics.forkManaged
      // Shutdown the queues first
      _       <- ZManaged.finalizer(q.shutdown)
      _       <- ZManaged.finalizer(q2.shutdown)
    } yield c

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

  private def dimension(name: String, value: String) = Dimension.builder().name(name).value(value).build()
}
