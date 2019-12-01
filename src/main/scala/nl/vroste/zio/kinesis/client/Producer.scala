package nl.vroste.zio.kinesis.client
import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.serde.Serializer
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry
import zio.{ Chunk, ZIO }
import zio.stream.ZSink

import scala.collection.JavaConverters._

object Producer {
  case class Queue[T](record: ProducerRecord[T])

  /**
   * Sink that produces records to Kinesis in an efficient and failsafe manner
   *
   * Records are buffered and sent in batches.
   * Records that failed to produce are retried.
   *
   * @param streamName
   * @param client
   * @param serializer
   * @tparam R
   * @tparam T
   * @return
   */
  def sink[R, T](
    streamName: String,
    client: Client,
    serializer: Serializer[R, T]
  ): ZSink[R, Throwable, Nothing, Chunk[ProducerRecord[T]], Unit] = {
    // Make a putRecords call and return failed records
    def produce(records: Chunk[ProducerRecord[T]]): ZIO[R, Throwable, Chunk[ProducerRecord[T]]] =
      client.putRecords(streamName, serializer, records.toVector).map { response =>
        val failed = Chunk
          .fromIterable(
            response
              .records()
              .asScala
          )
          .zipWith(records)(Tuple2.apply)
          .collect { case (result, record) if result.errorCode() != null => record }

        failed
      }

    def produceWithRetry(records: Chunk[ProducerRecord[T]]) = produce(records).flatMap { failed =>
      produce(failed)
    }

    ZSink.drain.contramapM(produceWithRetry)
  }
}
