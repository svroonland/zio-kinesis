package nl.vroste.zio.kinesis.interop.futures

import zio.aws.core.config
import izumi.reflect.Tag
import nl.vroste.zio.kinesis.client
import nl.vroste.zio.kinesis.client.Producer.ProduceResponse
import nl.vroste.zio.kinesis.client._
import nl.vroste.zio.kinesis.client.producer.ProducerMetrics
import nl.vroste.zio.kinesis.client.serde.Serializer
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClientBuilder
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder
import zio._

import scala.annotation.nowarn

/**
 * A scala-native Future based interface to the zio-kinesis Producer
 */
class Producer[T] private (
  runtime: zio.Runtime.Scoped[Any],
  producer: client.Producer[T],
  implicit val unsafe: Unsafe
) {

  /**
   * Produce a single record
   *
   * Backpressures when too many requests are in flight
   *
   * @param r
   * @return
   *   Task that fails if the records fail to be produced with a non-recoverable error
   */
  def produce(r: ProducerRecord[T]): CancelableFuture[ProduceResponse] =
    runtime.unsafe.runToFuture(producer.produce(r))

  /**
   * Backpressures when too many requests are in flight
   *
   * @return
   *   Task that fails if any of the records fail to be produced with a non-recoverable error
   */
  def produceMany(records: Iterable[ProducerRecord[T]]): CancelableFuture[Seq[ProduceResponse]] =
    runtime.unsafe.runToFuture(producer.produceChunk(Chunk.fromIterable(records)))

  /**
   * Shutdown the Producer
   */
  def close(): Unit = runtime.shutdown0()
}

object Producer {

  /**
   * Create a Producer of `T` values to stream `streamIdentifier`
   *
   * @param streamIdentifier
   *   Stream to produce to. Either just the name or the whole arn.
   * @param serializer
   *   Serializer for values of type T
   * @param settings
   * @param metricsCollector
   *   Periodically called with producer metrics
   * @tparam T
   *   Type of values to produce
   * @return
   *   A Managed Producer
   */
  @nowarn // Scala warns that Tag is unused, but removing it gives missing implicits errors
  def make[T: Tag](
    streamIdentifier: StreamIdentifier,
    serializer: Serializer[Any, T],
    settings: ProducerSettings = ProducerSettings(),
    metricsCollector: ProducerMetrics => Unit = (_: ProducerMetrics) => (),
    buildKinesisClient: KinesisAsyncClientBuilder => KinesisAsyncClientBuilder = identity,
    buildCloudWatchClient: CloudWatchAsyncClientBuilder => CloudWatchAsyncClientBuilder = identity,
    buildDynamoDbClient: DynamoDbAsyncClientBuilder => DynamoDbAsyncClientBuilder = identity,
    buildHttpClient: NettyNioAsyncHttpClient.Builder => SdkAsyncHttpClient = _.build()
  ): Producer[T] = {

    val sdkClients = HttpClientBuilder.make(build = buildHttpClient) >>> config.AwsConfig.default >>> (
      kinesisAsyncClientLayer(buildKinesisClient) ++
        cloudWatchAsyncClientLayer(buildCloudWatchClient) ++
        dynamoDbAsyncClientLayer(buildDynamoDbClient)
    )

    val producer = ZLayer.scoped {
      client.Producer
        .make(streamIdentifier, serializer, settings, metricsCollector = m => ZIO.attempt(metricsCollector(m)).orDie)
    }

    val layer = sdkClients >>> producer
    Unsafe.unsafe { implicit unsafe =>
      val runtime = zio.Runtime.unsafe.fromLayer(layer)

      new Producer[T](runtime, runtime.unsafe.run(ZIO.service[client.Producer[T]]).getOrThrow(), unsafe)
    }
  }
}
