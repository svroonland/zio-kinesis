package nl.vroste.zio.kinesis.client
import java.util.UUID

import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.TestUtil.retryOnResourceNotFound
import nl.vroste.zio.kinesis.client.serde.Serde
import zio.console._
import zio.duration._
import zio.stream.{ ZStream, ZTransducer }
import zio.{ Chunk, ExitCode, Schedule, ZIO }
import software.amazon.kinesis.exceptions.ShutdownException
import zio.UIO
import nl.vroste.zio.kinesis.client.zionative.FetchMode

object ExampleApp extends zio.App {

  override def run(
    args: List[String]
  ): ZIO[zio.ZEnv, Nothing, ExitCode] = {

    val streamName = "zio-test-stream-" + UUID.randomUUID().toString
    val nrRecords  = 200

    (for {
      client      <- Client.build(LocalStackDynamicConsumer.kinesisAsyncClientBuilder)
      adminClient <- AdminClient.build(LocalStackDynamicConsumer.kinesisAsyncClientBuilder)
    } yield (client, adminClient)).use {
      case (client, adminClient) =>
        for {
          _        <- TestUtil.createStreamUnmanaged(streamName, 10)
          _        <- produceRecords(streamName, nrRecords).fork
          _        <- UIO(println("Starting native consumer"))
          consumer <- zionative.Consumer
                        .shardedStream(
                          client,
                          adminClient,
                          LocalStackDynamicConsumer.dynamoDbClientBuilder.build(),
                          streamName,
                          applicationName = "testApp-2", // + UUID.randomUUID().toString(),
                          deserializer = Serde.asciiString,
                          fetchMode = FetchMode.Polling(1000)
                        )
                        .flatMapPar(Int.MaxValue) {
                          case (shardID, shardStream, checkpointer) =>
                            shardStream
                              .tap(r =>
                                checkpointer
                                  .stageOnSuccess(putStrLn(s"Processing record $r").when(false))(r)
                              )
                              .aggregateAsyncWithin(ZTransducer.last, Schedule.fixed(1.second))
                              .mapConcat(_.toList)
                              .tap { _ =>
                                putStrLn(s"Checkpointing ${shardID}") *> checkpointer.checkpoint
                              }
                              .catchSome {
                                // This happens when the lease for the shard is lost. Best we can do is end the stream.
                                case _: ShutdownException => ZStream.empty
                              }
                        }
                        // .timeout(5.seconds)
                        .runCollect
                        .fork
          // _        <- ZIO.sleep(10.seconds)
          _        <- consumer.join
        } yield ExitCode.success
    }.orDie
  }

  def produceRecords(streamName: String, nrRecords: Int) =
    (for {
      client   <- Client.build(LocalStackDynamicConsumer.kinesisAsyncClientBuilder)
      producer <- Producer.make(streamName, client, Serde.asciiString)
    } yield producer).use { producer =>
      val records =
        (1 to nrRecords).map(i => ProducerRecord(s"key$i", s"msg$i"))
      ZStream
        .fromIterable(records)
        .chunkN(10)
        .mapChunksM(
          producer
            .produceChunk(_)
            .tapError(e => putStrLn(s"error: $e").provideLayer(Console.live))
            .retry(retryOnResourceNotFound)
            .as(Chunk.unit)
            .delay(1.second)
        )
        .runDrain
    }
}
