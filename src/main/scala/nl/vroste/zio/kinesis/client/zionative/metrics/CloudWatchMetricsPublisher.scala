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
import zio.Ref
import zio.stream.Take
import zio.Chunk
import nl.vroste.zio.kinesis.client.Util

case class CloudWatchMetricsPublisherConfig(
  flushInterval: Duration = 20.seconds,
  maxBatchSize: Int = 20,
  retrySchedule: Schedule[Clock, Any, (Duration, Int)] = Util.exponentialBackoff(1.second, 1.minute)
) {
  require(maxBatchSize <= 20, "maxBatchSize must be <= 20 (AWS SDK limit)")
}

/**
 * Publishes KCL compatible metrics to CloudWatch
 *
 * TODO how do we support an application where there's more than one Kinesis stream
 * being processed, so multiple sharded streams and multiple LeaseRepository and
 *
 *
 *
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
      case LeaseAcquired(shardId, checkpoint)                       => List.empty
      case ShardLeaseLost(shardId)                                  =>
        List.empty
        List(
          metric(
            "LostLeases",
            1,
            timestamp,
            Seq("WorkerIdentifier" -> workerId, "Operation" -> "RenewAllLeases"),
            StandardUnit.COUNT
          )
        )
      case LeaseRenewed(shardId, duration)                          =>
        List(
          metric(
            "RenewLease.Time",
            duration.toMillis,
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
      case LeaseReleased(shardId)                                   => List.empty
      case Checkpoint(shardId, checkpoint)                          => List.empty
      case WorkerJoined(workerId)                                   => List.empty
      case WorkerLeft(workerId)                                     => List.empty
      // TODO LeaseCreated (for new leases)
      // TODO count the number of current leases (periodic metric based on LeaseAcquired and ShardLeaseLost+LeaseReleased)
      // TODO lease taken
      // TODO NumWorkers (periodic metric based on WorkerJoined and WorkerLeft)
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

  def collectPeriodicMetrics(event: DiagnosticEvent, timestamp: Instant): UIO[Unit] =
    event match {
      case PollComplete(shardId, nrRecords, behindLatest, duration) => UIO.unit
      case SubscribeToShardEvent(shardId, nrRecords, behindLatest)  => UIO.unit

      case LeaseAcquired(shardId, checkpoint) => heldLeases.update(_ + shardId)
      case ShardLeaseLost(shardId)            => heldLeases.update(_ - shardId)
      case LeaseRenewed(shardId, duration)    => UIO.unit
      case LeaseReleased(shardId)             => heldLeases.update(_ - shardId)
      case Checkpoint(shardId, checkpoint)    => UIO.unit
      case WorkerJoined(workerId)             => workers.update(_ + workerId)
      case WorkerLeft(workerId)               => workers.update(_ - workerId)
    }

  val processQueue =
    (ZStream
      .fromQueue(eventQueue)
      .mapM(event => now.map((event, _)))
      .tap(Function.tupled(collectPeriodicMetrics))
      .mapConcat(Function.tupled(toMetrics)) merge ZStream.fromQueue(periodicMetricsQueue))
      .aggregateAsyncWithin(ZTransducer.collectAllN(config.maxBatchSize), Schedule.fixed(config.flushInterval))
      .mapM(putMetricData)             // TODO should probably retry
      .runDrain
      .retry(Schedule.fixed(1.second)) // TODO proper back off

  val generatePeriodicMetrics =
    (for {
      now            <- now
      nrWorkers      <- workers.get.map(_.size)
      nrLeases       <- heldLeases.get.map(_.size)
      _               = println(s"Worker ${workerId} has ${nrLeases} leases")
      nrWorkersMetric = metric(
                          "NumWorkers",
                          nrWorkers,
                          now,
                          Seq("Operation" -> "TakeLeases", "WorkerIdentifier" -> workerId),
                          StandardUnit.COUNT
                        )
      nrLeasesMetric  = metric(
                         "TotalLeases",
                         nrLeases,
                         now,
                         Seq("Operation" -> "TakeLeases", "WorkerIdentifier" -> workerId),
                         StandardUnit.COUNT
                       )
      nrLeasesMetric2 = metric(
                          "CurrentLeases",
                          nrLeases,
                          now,
                          Seq("Operation" -> "RenewAllLeases", "WorkerIdentifier" -> workerId),
                          StandardUnit.COUNT
                        )
      _              <- periodicMetricsQueue.offerAll(List(nrWorkersMetric, nrLeasesMetric, nrLeasesMetric2))
    } yield ()).repeat(Schedule.fixed(30.seconds)) // TODO configurable

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
      _       <- ZManaged.finalizer(q.shutdown)
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
