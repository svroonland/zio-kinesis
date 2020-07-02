package nl.vroste.zio.kinesis.client

import software.amazon.awssdk.services.kinesis.model.{ ResourceInUseException, ResourceNotFoundException }
import zio.clock.Clock
import zio.console.{ putStrLn, Console }
import zio.duration._
import zio.{ Schedule, ZIO, ZManaged }

object TestUtil {

  def createStream(streamName: String, nrShards: Int): ZManaged[Console with AdminClient, Throwable, Unit] =
    for {
      adminClient <- ZManaged.service[AdminClient.Service]
      _           <- adminClient
             .createStream(streamName, nrShards)
             .catchSome {
               case _: ResourceInUseException =>
                 putStrLn("Stream already exists")
             }
             .toManaged { _ =>
               adminClient
                 .deleteStream(streamName, enforceConsumerDeletion = true)
                 .catchSome {
                   case _: ResourceNotFoundException => ZIO.unit
                 }
                 .orDie
             }
    } yield ()

  def createStreamUnmanaged(streamName: String, nrShards: Int): ZIO[Console with AdminClient, Throwable, Unit] =
    for {
      adminClient <- ZIO.service[AdminClient.Service]
      _           <- adminClient.createStream(streamName, nrShards).catchSome {
             case _: ResourceInUseException =>
               putStrLn("Stream already exists")
           }
    } yield ()

  val retryOnResourceNotFound: Schedule[Clock, Throwable, ((Throwable, Int), Duration)] =
    Schedule.doWhile[Throwable] {
      case _: ResourceNotFoundException => true
      case _                            => false
    } &&
      Schedule.recurs(5) &&
      Schedule.exponential(2.second)

  def recordsForBatch(batchIndex: Int, batchSize: Int): Seq[Int] =
    ((if (batchIndex == 1) 1 else (batchIndex - 1) * batchSize) to (batchSize * batchIndex) - 1)

//  def putRecordsEmitter(
//    streamName: String,
//    batchSize: Int,
//    max: Int
//  ) =
//    ZStream.unfoldM(0) { i =>
//      ZIO.service[Client.Service].flatMap { client =>
//        if (i < max) {
//          val recordsBatch                                           =
//            (i until i + batchSize)
//              .map(_ => ProducerRecord(s"key$i", s"msg$i"))
//          val putRecordsM: ZIO[Clock, Throwable, PutRecordsResponse] = client
//            .putRecords(
//              streamName,
//              Serde.asciiString,
//              recordsBatch
//            )
//            .retry(retryOnResourceNotFound)
//          for {
//            _ <- putStrLn(s"i=$i putting $batchSize  records into Kinesis")
//            _ <- putRecordsM
//            _ <- putStrLn(s"successfully put records")
//          } yield Some((i, i + batchSize))
//        } else
//          ZIO.effectTotal(None)
//      }
//    }

}
