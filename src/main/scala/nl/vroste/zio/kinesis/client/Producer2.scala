//package nl.vroste.zio.kinesis.client
//
//import nl.vroste.zio.kinesis.client.Client.ProducerRecord
//import nl.vroste.zio.kinesis.client.Producer.ProduceResponse
//import zio.clock.Clock
//import zio.duration.Duration
//import zio.stream.ZSink
//import zio.{ Chunk, Has, Schedule, Task }
//import zio.duration._
//
//object Producer2 {
//  type Producer2[T] = Has[Service[T]]
//
//  /**
//   * Producer for Kinesis records
//   *
//   * Supports higher volume producing than making use of the [[Client]] directly.
//   *
//   * Features:
//   * - Batching of records into a single PutRecords calls to Kinesis for reduced IO overhead
//   * - Retry requests with backoff on recoverable errors
//   * - Retry individual records
//   * - Rate limiting to respect shard capacity (TODO)
//   *
//   * Records are batched for up to `maxBufferDuration` time, or 500 records or 5MB of payload size,
//   * whichever comes first. The latter two are Kinesis API limits.
//   *
//   * Individual records which cannot be produced due to Kinesis shard rate limits are retried.
//   *
//   * Individual shard rate limiting is not yet implemented by this library.
//   *
//   * Inspired by https://docs.aws.amazon.com/streams/latest/dev/developing-producers-with-kpl.html and
//   * https://aws.amazon.com/blogs/big-data/implementing-efficient-and-reliable-producers-with-the-amazon-kinesis-producer-library/
//   *
//   * Rate limits for the Kinesis PutRecords API (see https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html):
//   * - 500 records per request
//   * - Whole request max 5MB
//   * - Each item max 1MB
//   * - Each shard max 1000 records / s
//   * - Each shard max 1MB / s
//   */
//  trait Service[T] {
//
//    /**
//     * Produce a single record
//     *
//       * Backpressures when too many requests are in flight
//     *
//       * @param r
//     * @return Task that fails if the records fail to be produced with a non-recoverable error
//     */
//    def produce(r: ProducerRecord[T]): Task[ProduceResponse]
//
//    /**
//     * Backpressures when too many requests are in flight
//     *
//       * @return Task that fails if any of the records fail to be produced with a non-recoverable error
//     */
//    def produceChunk(chunk: Chunk[ProducerRecord[T]]): Task[List[ProduceResponse]]
//
//    /**
//     * ZSink interface to the Producer
//     */
//    def sinkChunked: ZSink[Any, Throwable, Chunk[ProducerRecord[T]], Unit] =
//      ZSink.drain.contramapM(produceChunk)
//  }
//
//  final case class ProducerSettings(
//    bufferSize: Int = 8192, // Prefer powers of 2
//    maxBufferDuration: Duration = 500.millis,
//    maxParallelRequests: Int = 24,
//    backoffRequests: Schedule[Clock, Throwable, Any] = Schedule.exponential(500.millis) && Schedule.recurs(5),
//    failedDelay: Duration = 100.millis
//  )
//
//}
