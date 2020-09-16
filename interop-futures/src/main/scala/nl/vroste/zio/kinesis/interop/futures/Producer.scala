package nl.vroste.zio.kinesis.interop.futures

import izumi.reflect.Tag
import nl.vroste.zio.kinesis.client
import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.Producer.ProduceResponse
import nl.vroste.zio.kinesis.client._
import nl.vroste.zio.kinesis.client.producer.ProducerMetrics
import nl.vroste.zio.kinesis.client.serde.Serializer
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.cloudwatch.{ CloudWatchAsyncClient, CloudWatchAsyncClientBuilder }
import software.amazon.awssdk.services.dynamodb.{ DynamoDbAsyncClient, DynamoDbAsyncClientBuilder }
import software.amazon.awssdk.services.kinesis.{ KinesisAsyncClient, KinesisAsyncClientBuilder }
import zio.clock.Clock
import zio.logging.Logging
import zio.{ CancelableFuture, Chunk, ZIO }

/**
 * A scala-native Future based interface to the zio-kinesis Producer
 */
class Producer[T] private (
  runtime: zio.Runtime[Any],
  producer: client.Producer[T]
) {

  /**
   * Produce a single record
   *
   * Backpressures when too many requests are in flight
   *
   * @param r
   * @return Task that fails if the records fail to be produced with a non-recoverable error
   */
  def produce(r: ProducerRecord[T]): CancelableFuture[ProduceResponse] =
    runtime.unsafeRunToFuture(producer.produce(r))

  /**
   * Backpressures when too many requests are in flight
   *
   * @return Task that fails if any of the records fail to be produced with a non-recoverable error
   */
  def produceMany(records: Iterable[ProducerRecord[T]]): CancelableFuture[Seq[ProduceResponse]] =
    runtime.unsafeRunToFuture(producer.produceChunk(Chunk.fromIterable(records)))
}

object Producer {

  /**
   * Create a Producer of `T` values to stream `streamName`
   *
   * @param streamName Stream to produce to
   * @param serializer Serializer for values of type T
   * @param settings
   * @param metricsCollector Periodically called with producer metrics
   * @tparam T Type of values to produce
   * @return A Managed Producer
   */
  def make[T: Tag](
    streamName: String,
    serializer: Serializer[Any, T],
    settings: ProducerSettings = ProducerSettings(),
    metricsCollector: ProducerMetrics => Unit = (_: ProducerMetrics) => (),
    buildKinesisClient: KinesisAsyncClientBuilder => KinesisAsyncClient = _.build(),
    buildCloudWatchClient: CloudWatchAsyncClientBuilder => CloudWatchAsyncClient = _.build(),
    buildDynamoDbClient: DynamoDbAsyncClientBuilder => DynamoDbAsyncClient = _.build(),
    buildHttpClient: NettyNioAsyncHttpClient.Builder => SdkAsyncHttpClient = _.build()
  ): Producer[T] = {

    val sdkClients = HttpClient.make(build = buildHttpClient) >>> (
      kinesisAsyncClientLayer(buildKinesisClient) ++
        cloudWatchAsyncClientLayer(buildCloudWatchClient) ++
        dynamoDbAsyncClientLayer(buildDynamoDbClient)
    )

    val producer =
      client.Producer
        .make(streamName, serializer, settings, metricsCollector = m => ZIO(metricsCollector(m)).orDie)
        .toLayer

    val layer   = (Clock.live ++ Logging.ignore ++ (sdkClients >>> Client.live)) >>> producer
    val runtime = zio.Runtime.unsafeFromLayer(layer)

    new Producer[T](runtime, runtime.unsafeRun(ZIO.service[client.Producer[T]]))
  }
}
