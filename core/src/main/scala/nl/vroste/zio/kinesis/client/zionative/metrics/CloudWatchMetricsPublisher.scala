package nl.vroste.zio.kinesis.client.zionative.metrics

import nl.vroste.zio.kinesis.client.Util
import nl.vroste.zio.kinesis.client.zionative.DiagnosticEvent
import nl.vroste.zio.kinesis.client.zionative.DiagnosticEvent._
import zio.aws.cloudwatch.CloudWatch
import zio.aws.cloudwatch.model.primitives._
import zio.aws.cloudwatch.model.{ Dimension, MetricDatum, PutMetricDataRequest, StandardUnit }
import zio.stream.{ ZSink, ZStream }
import zio.{ Clock, _ }

import java.time.Instant

/**
 * Configuration for CloudWatch metrics publishing
 *
 * @param maxFlushInterval
 *   Collected metrics will be uploaded to CloudWatch at most this interval
 * @param maxBatchSize
 *   Collected metrics will be uploaded to CloudWatch at most this number of metrics. Must be <= 20 (AWS SDK limit)
 * @param periodicMetricInterval
 *   Periodic metrics (nr leases / nr workers) are collected at this interval
 * @param retrySchedule
 *   Transient upload failures are retried according to this schedule
 * @param maxParallelUploads
 *   The maximum number of in-flight requests with batches of metric data to CloudWatch
 */
final case class CloudWatchMetricsPublisherConfig(
  maxFlushInterval: Duration = 20.seconds,
  maxBatchSize: Int = 20,
  periodicMetricInterval: Duration = 30.seconds,
  retrySchedule: Schedule[Clock, Any, (Duration, Long)] = Util.exponentialBackoff(1.second, 1.minute),
  maxParallelUploads: Int = 3
) {
  require(maxBatchSize <= 20, "maxBatchSize must be <= 20 (AWS SDK limit)")
}

/**
 * Publishes KCL compatible metrics to CloudWatch
 */
private class CloudWatchMetricsPublisher(
  client: CloudWatch,
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
            StandardUnit.Count
          )
        )
      case LeaseRenewed(shardId @ _, duration)                      =>
        List(
          metric(
            "RenewLease.Time",
            duration.toMillis.toDouble,
            timestamp,
            Seq("WorkerIdentifier" -> workerId, "Operation" -> "RenewAllLeases"),
            StandardUnit.Milliseconds
          ),
          metric(
            "RenewLease.Success",
            1,
            timestamp,
            Seq("WorkerIdentifier" -> workerId, "Operation" -> "RenewAllLeases"),
            StandardUnit.Count
          )
        )
      case LeaseReleased(shardId @ _)                               => List.empty // Processed in periodic metrics
      case NewShardDetected(shardId @ _)                            => List.empty
      case ShardEnded(shard @ _)                                    => List.empty
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
        StandardUnit.Count
      ), // TODO fetched != quite processed
      metric(
        "Time",
        duration.toMillis.toDouble,
        timestamp,
        Seq("ShardId" -> shardId, "Operation" -> "ProcessTask"),
        StandardUnit.Milliseconds
      ),
      metric(
        "MillisBehindLatest",
        behindLatest.toMillis.toDouble,
        timestamp,
        Seq("ShardId" -> shardId, "Operation" -> "ProcessTask"),
        StandardUnit.Milliseconds
      )
    )

  val now = zio.Clock.currentDateTime.map(_.toInstant())

  def collectPeriodicMetrics(event: DiagnosticEvent): UIO[Unit] =
    event match {
      case PollComplete(_, _, _, _)       => UIO.unit
      case SubscribeToShardEvent(_, _, _) => UIO.unit

      case LeaseAcquired(shardId, _) => heldLeases.update(_ + shardId)
      case ShardLeaseLost(shardId)   => heldLeases.update(_ - shardId)
      case LeaseRenewed(_, _)        => UIO.unit
      case LeaseReleased(shardId)    => heldLeases.update(_ - shardId)
      case ShardEnded(_)             => UIO.unit
      case NewShardDetected(_)       => UIO.unit
      case Checkpoint(_, _)          => UIO.unit
      case WorkerJoined(workerId)    => workers.update(_ + workerId)
      case WorkerLeft(workerId)      => workers.update(_ - workerId)
    }

  val processQueue =
    (ZStream
      .fromQueue(eventQueue)
      .mapZIO(event => now.map((event, _)))
      .tap { case (e, _) => collectPeriodicMetrics(e) }
      .mapConcat(Function.tupled(toMetrics)) merge ZStream.fromQueue(periodicMetricsQueue))
      .aggregateAsyncWithin(
        ZSink.collectAllN[MetricDatum](config.maxBatchSize),
        Schedule.fixed(config.maxFlushInterval)
      )
      .mapZIOParUnordered(config.maxParallelUploads) { metrics =>
        putMetricData(metrics)
          .tapError(e => ZIO.logWarning(s"Failed to upload metrics, will retry: ${e}"))
          .retry(config.retrySchedule)
          .orDie // orDie because schedule has Any as error type?
      }
      .runDrain
      .tapErrorCause(e => ZIO.logSpan("Metrics uploading has stopped with error")(ZIO.logErrorCause(e)))

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
                          StandardUnit.Count
                        )
      nrLeasesMetric  = metric(
                          "TotalLeases",
                          nrLeases.toDouble,
                          now,
                          Seq("Operation" -> "TakeLeases", "WorkerIdentifier" -> workerId),
                          StandardUnit.Count
                        )
      nrLeasesMetric2 = metric(
                          "CurrentLeases",
                          nrLeases.toDouble,
                          now,
                          Seq("Operation" -> "RenewAllLeases", "WorkerIdentifier" -> workerId),
                          StandardUnit.Count
                        )
      _              <- periodicMetricsQueue.offerAll(List(nrWorkersMetric, nrLeasesMetric, nrLeasesMetric2))
    } yield ()).repeat(Schedule.fixed(config.periodicMetricInterval))

  private def putMetricData(metricData: Seq[MetricDatum]): ZIO[Any, Throwable, Unit] = {
    val request = PutMetricDataRequest(Namespace(namespace), metricData.toList)

    client
      .putMetricData(request)
      .unit
      .mapError(_.toThrowable)
  }
}

object CloudWatchMetricsPublisher {
  trait Service {
    def processEvent(e: DiagnosticEvent): UIO[Unit]
  }

  def make(
    applicationName: String,
    workerId: String
  ): ZIO[Scope with Clock with CloudWatch with CloudWatchMetricsPublisherConfig, Nothing, Service] =
    for {
      client  <- ZIO.service[CloudWatch]
      config  <- ZIO.service[CloudWatchMetricsPublisherConfig]
      q       <- Queue.bounded[DiagnosticEvent](1000)
      q2      <- Queue.bounded[MetricDatum](1000)
      leases  <- Ref.make[Set[String]](Set.empty)
      workers <- Ref.make[Set[String]](Set.empty)
      c        = new CloudWatchMetricsPublisher(client, q, q2, applicationName, workerId, leases, workers, config)
      _       <- c.processQueue.forkScoped
      _       <- c.generatePeriodicMetrics.forkScoped
      // Shutdown the queues first
      _       <- ZIO.addFinalizer(q.shutdown)
      _       <- ZIO.addFinalizer(q2.shutdown)
    } yield c

  private def metric(
    name: String,
    value: Double,
    timestamp: Instant,
    dimensions: Seq[(String, String)],
    unit: StandardUnit
  ): MetricDatum =
    MetricDatum(
      MetricName(name),
      Some(dimensions.map { case (name, value) => Dimension(DimensionName(name), DimensionValue(value)) }.toList),
      Some(Timestamp(timestamp)),
      Some(DatapointValue(value)),
      unit = Some(unit)
    )
}
