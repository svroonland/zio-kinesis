package nl.vroste.zio.kinesis.interop.futures
import io.github.vigoo.zioaws.core.config
import io.github.vigoo.zioaws.kinesis.Kinesis
import nl.vroste.zio.kinesis.client._
import nl.vroste.zio.kinesis.client.serde.Deserializer
import nl.vroste.zio.kinesis.client.zionative.Consumer.InitialPosition
import nl.vroste.zio.kinesis.client.zionative.leasecoordinator.LeaseCoordinationSettings
import nl.vroste.zio.kinesis.client.zionative.leaserepository.DynamoDbLeaseRepository
import nl.vroste.zio.kinesis.client.zionative.{ DiagnosticEvent, FetchMode, LeaseRepository, ShardAssignmentStrategy }
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClientBuilder
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder
import zio.clock.Clock
import zio.logging.Logging
import zio.random.Random
import zio.{ CancelableFuture, ZIO }

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

/**
 * A scala-native Future based interface to the zio-kinesis Consumer
 */
class Consumer private (
  runtime: zio.Runtime.Managed[Clock with Random with Kinesis with LeaseRepository with Logging]
) {

  /**
   * Apply an effectful function to each record in a stream
   *
   * This is the easiest way to consume Kinesis records from a stream, while benefiting from all of Consumer's features
   * like parallel streaming, checkpointing and resharding.
   *
   * Simply provide an asynchronous function that is applied to each record and the rest is taken care of. The function
   * will be called for every record in the stream, with a parallelism.
   * @param checkpointBatchSize
   *   Maximum number of records before checkpointing
   * @param checkpointDuration
   *   Maximum interval before checkpointing
   * @param recordProcessor
   *   A function for processing a `Record[T]`
   * @tparam T
   *   Type of record values
   * @return
   *   A cancelable future that completes with Unit when record processing is stopped or fails when the consumer stream
   *   fails
   */
  def consumeWith[T](
    streamName: String,
    applicationName: String,
    deserializer: Deserializer[Any, T],
    workerIdentifier: String = "worker1",
    fetchMode: FetchMode = FetchMode.Polling(),
    leaseCoordinationSettings: LeaseCoordinationSettings = LeaseCoordinationSettings(),
    initialPosition: InitialPosition = InitialPosition.TrimHorizon,
    emitDiagnostic: DiagnosticEvent => Unit = (_: DiagnosticEvent) => (),
    shardAssignmentStrategy: ShardAssignmentStrategy = ShardAssignmentStrategy.balanced(),
    checkpointBatchSize: Long = 200,
    checkpointDuration: Duration = 5.minutes
  )(
    recordProcessor: Record[T] => ExecutionContext => Future[Unit]
  ): CancelableFuture[Unit] =
    runtime.unsafeRunToFuture {
      zionative.Consumer.consumeWith(
        streamName,
        applicationName,
        deserializer,
        workerIdentifier,
        fetchMode,
        leaseCoordinationSettings,
        initialPosition,
        emitDiagnostic = e => ZIO(emitDiagnostic(e)).orDie,
        shardAssignmentStrategy,
        checkpointBatchSize,
        zio.duration.Duration.fromScala(checkpointDuration)
      )(record => ZIO.fromFuture(recordProcessor(record)))
    }

  def close(): Unit = runtime.shutdown()
}

object Consumer {
  def make(
    buildKinesisClient: KinesisAsyncClientBuilder => KinesisAsyncClientBuilder = identity,
    buildCloudWatchClient: CloudWatchAsyncClientBuilder => CloudWatchAsyncClientBuilder = identity,
    buildDynamoDbClient: DynamoDbAsyncClientBuilder => DynamoDbAsyncClientBuilder = identity,
    buildHttpClient: NettyNioAsyncHttpClient.Builder => SdkAsyncHttpClient = _.build()
  ): Consumer = {

    val sdkClients = HttpClientBuilder.make(build = buildHttpClient) >>> config.default >>> (
      kinesisAsyncClientLayer(buildKinesisClient) ++
        cloudWatchAsyncClientLayer(buildCloudWatchClient) ++
        dynamoDbAsyncClientLayer(buildDynamoDbClient)
    )

    val layer = Clock.live ++
      Random.live ++
      Logging.ignore ++
      (sdkClients >+> DynamoDbLeaseRepository.live)

    val runtime = zio.Runtime.unsafeFromLayer(layer)

    new Consumer(runtime)
  }
}
