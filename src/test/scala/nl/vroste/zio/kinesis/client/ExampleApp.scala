package nl.vroste.zio.kinesis.client
import java.util.UUID

import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.TestUtil.retryOnResourceNotFound
import nl.vroste.zio.kinesis.client.serde.Serde
import zio.console._
import zio.duration._
import zio.stream.{ ZStream, ZTransducer }
import zio.{ Chunk, ExitCode, Schedule, ZIO }
import zio.UIO
import nl.vroste.zio.kinesis.client.zionative.FetchMode
import nl.vroste.zio.kinesis.client.zionative.Consumer
import TestUtil.Layers
import nl.vroste.zio.kinesis.client.zionative.ShardLeaseLost

object ExampleApp extends zio.App {

  override def run(
    args: List[String]
  ): ZIO[zio.ZEnv, Nothing, ExitCode] = {

    val streamName = "zio-test-stream-" + UUID.randomUUID().toString
    val nrRecords  = 200

    for {
      _        <- TestUtil.createStreamUnmanaged(streamName, 10)
      _        <- produceRecords(streamName, nrRecords).fork
      _        <- UIO(println("Starting native consumer"))
      consumer <- Consumer
                    .shardedStream(
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
                          .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
                          .tap { _ =>
                            putStrLn(s"Checkpointing ${shardID}") *> checkpointer.checkpoint
                          }
                          .catchAll {
                            case Right(ShardLeaseLost) =>
                              ZStream.empty
                            case Left(e)               => ZStream.fail(e)
                          }
                    }
                    // .timeout(5.seconds)
                    .runCollect
                    .fork
      // _        <- ZIO.sleep(10.seconds)
      _        <- consumer.join
    } yield ExitCode.success
  }.orDie.provideCustomLayer(
    (Layers.kinesisAsyncClient >>> (Layers.adminClient ++ Layers.client)).orDie ++ Layers.dynamo.orDie
  )

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
