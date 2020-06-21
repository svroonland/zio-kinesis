//package nl.vroste.zio.kinesis.client
//
//import java.time.Instant
//
//import izumi.reflect.Tag
//import nl.vroste.zio.kinesis.client.Producer.ProduceResponse
//import nl.vroste.zio.kinesis.client.Client.ProducerRecord
//import nl.vroste.zio.kinesis.client.Producer2.Producer2
//import nl.vroste.zio.kinesis.client.serde.Serializer
//import software.amazon.awssdk.core.SdkBytes
//import software.amazon.awssdk.services.kinesis.model.{ KinesisException, PutRecordsRequestEntry }
//import zio.clock.Clock
//import zio.stream.{ ZStream, ZTransducer }
//import zio._
//
//import scala.jdk.CollectionConverters._
//import scala.util.control.NonFatal
//import Producer._
//import nl.vroste.zio.kinesis.client.Client2.Client2
//
//object Producer2Live {
//  def layer[T](implicit tag: Tag[Producer2.Service[T]]): Layer[Nothing, Has[Producer2.Service[T]]] =
//    ZLayer.succeed {
//      new Producer2.Service[T] {
//
//        /**
//         * Produce a single record
//         *
//         * Backpressures when too many requests are in flight
//         *
//         * @param r
//         * @return Task that fails if the records fail to be produced with a non-recoverable error
//         */
//        override def produce(r: Client.ProducerRecord[T]): Task[Producer.ProduceResponse] = ???
//
//        /**
//         * Backpressures when too many requests are in flight
//         *
//         * @return Task that fails if any of the records fail to be produced with a non-recoverable error
//         */
//        override def produceChunk(chunk: Chunk[Client.ProducerRecord[T]]): Task[List[Producer.ProduceResponse]] = ???
//      }
//    }
//
//  /*
//    def layer[T](
//      streamName: String,
//      client: Client,
//      settings: ProducerSettings = ProducerSettings()
//    ): ZLayer[Serializer[T] with Client2, Throwable, Producer[T]] =
//    ZLayer.fromFunctionManaged[Client2, Nothing, Producer2.Service[T]] { hasClient =>
//   */
//  def x[T]: ZLayer[Clock, Throwable, Producer[T]] = ???
//
//  def makeLayer[R, T](
//    streamName: String,
//    serializer: Serializer[R, T],
//    settings: ProducerSettings = ProducerSettings()
//  )(implicit tag: Tag[Producer2.Service[T]]): ZLayer[Client2, Nothing, Has[Producer2.Service[T]]] =
//    ZLayer.fromFunctionManaged[Client2, Nothing, Producer2.Service[T]] { hasClient =>
//      val value: ZManaged[R with Clock, Nothing, Producer2.Service[T]] = for {
//        env   <- ZIO.environment[R with Clock].toManaged_
//        queue <- zio.Queue.bounded[ProduceRequest](settings.bufferSize).toManaged(_.shutdown)
//
//        failedQueue <- zio.Queue.bounded[ProduceRequest](settings.bufferSize).toManaged(_.shutdown)
//
//        // Failed records get precedence)
//        _ <- (ZStream.fromQueue(failedQueue) merge ZStream.fromQueue(queue))
//             // Buffer records up to maxBufferDuration or up to the Kinesis PutRecords request limit
//               .aggregateAsyncWithin(
//                 ZTransducer.fold(PutRecordsBatch.empty)(_.isWithinLimits)(_.add(_)),
//                 Schedule.spaced(settings.maxBufferDuration)
//               )
//               // Several putRecords requests in parallel
//               .mapMPar(settings.maxParallelRequests) { batch: PutRecordsBatch =>
//                 (for {
//                   response              <- hasClient.get
//                                 .putRecords(streamName, batch.entries.map(_.r))
//                                 .retry(scheduleCatchRecoverable && settings.backoffRequests)
//
//                   maybeSucceeded         = response
//                                      .records()
//                                      .asScala
//                                      .zip(batch.entries)
//                   (newFailed, succeeded) = if (response.failedRecordCount() > 0)
//                                              maybeSucceeded.partition {
//                                                case (result, _) =>
//                                                  result.errorCode() != null && recoverableErrorCodes.contains(
//                                                    result.errorCode()
//                                                  )
//                                              }
//                                            else
//                                              (Seq.empty, maybeSucceeded)
//
//                   // TODO backoff for shard limit stuff
//                   _                     <- failedQueue
//                          .offerAll(newFailed.map(_._2))
//                          .delay(settings.failedDelay)
//                          .fork // TODO should be per shard
//                   _                     <- ZIO.foreach(succeeded) {
//                          case (response, request) =>
//                            request.done.succeed(ProduceResponse(response.shardId(), response.sequenceNumber()))
//                        }
//                 } yield ()).catchAll { case NonFatal(e) => ZIO.foreach_(batch.entries.map(_.done))(_.fail(e)) }
//               }
//               .runDrain
//               .toManaged_
//               .fork
//      } yield new Producer2.Service[T] {
//        override def produce(r: ProducerRecord[T]): Task[ProduceResponse] =
//          for {
//            now      <- zio.clock.currentDateTime.provide(env)
//            done     <- Promise.make[Throwable, ProduceResponse]
//            data     <- serializer.serialize(r.data).provide(env)
//            entry     = PutRecordsRequestEntry
//                      .builder()
//                      .partitionKey(r.partitionKey)
//                      .data(SdkBytes.fromByteBuffer(data))
//                      .build()
//            request   = ProduceRequest(entry, done, now.toInstant)
//            _        <- queue.offer(request)
//            response <- done.await
//          } yield response
//
//        override def produceChunk(chunk: Chunk[ProducerRecord[T]]): Task[List[ProduceResponse]] =
//          zio.clock.currentDateTime
//            .provide(env)
//            .flatMap { now =>
//              ZIO
//                .foreach(chunk.toList) { r =>
//                  for {
//                    done <- Promise.make[Throwable, ProduceResponse]
//                    data <- serializer.serialize(r.data).provide(env)
//                    entry = PutRecordsRequestEntry
//                              .builder()
//                              .partitionKey(r.partitionKey)
//                              .data(SdkBytes.fromByteBuffer(data))
//                              .build()
//                  } yield ProduceRequest(entry, done, now.toInstant)
//                }
//            }
//            .flatMap(requests => queue.offerAll(requests) *> ZIO.foreachPar(requests)(_.done.await))
//      }
//      value
//
//    }
//
////  val maxRecordsPerRequest     = 500             // This is a Kinesis API limitation
////  val maxPayloadSizePerRequest = 5 * 1024 * 1024 // 5 MB
////
////  val recoverableErrorCodes = Set("ProvisionedThroughputExceededException", "InternalFailure", "ServiceUnavailable");
//
////  final case class ProduceResponse(shardId: String, sequenceNumber: String)
//
//  private final case class ProduceRequest(
//    r: PutRecordsRequestEntry,
//    done: Promise[Throwable, ProduceResponse],
//    timestamp: Instant
//  )
//
//  private final case class PutRecordsBatch(entries: List[ProduceRequest], nrRecords: Int, payloadSize: Long) {
//    def add(entry: ProduceRequest): PutRecordsBatch =
//      copy(
//        entries = entry +: entries,
//        nrRecords = nrRecords + 1,
//        payloadSize = payloadSize + entry.r.partitionKey().length + entry.r.data().asByteArray().length
//      )
//
//    def isWithinLimits =
//      nrRecords <= maxRecordsPerRequest &&
//        payloadSize < maxPayloadSizePerRequest
//  }
//
//  private object PutRecordsBatch {
//    val empty = PutRecordsBatch(List.empty, 0, 0)
//  }
//
//  private final def scheduleCatchRecoverable: Schedule[Any, Throwable, Throwable] =
//    Schedule.doWhile {
//      case e: KinesisException if e.statusCode() / 100 != 4 => true
//      case _                                                => false
//    }
//
//}
