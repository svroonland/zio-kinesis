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
import zio.logging.slf4j.Slf4jLogger
import zio.logging.log
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder
import zio.ZLayer
import zio.Has
import zio.ZManaged
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import nl.vroste.zio.kinesis.client.zionative.dynamodb.LeaseCoordinationSettings

object ExampleApp extends zio.App {

  override def run(
    args: List[String]
  ): ZIO[zio.ZEnv, Nothing, ExitCode] = {

    val streamName      = "zio-test-stream-2" // + UUID.randomUUID().toString
    val nrRecords       = 200000
    val nrShards        = 21
    val applicationName = "testApp-4"         // + UUID.randomUUID().toString(),

    def worker(id: String) =
      ZStream.fromEffect(zio.random.nextIntBetween(0, 3000).flatMap(d => ZIO.sleep(d.millis))) *> Consumer
        .shardedStream(
          streamName,
          applicationName = applicationName,
          deserializer = Serde.asciiString,
          fetchMode = FetchMode.EnhancedFanOut,
          emitDiagnostic = ev => log.info(id + ": " + ev.toString()).provideLayer(loggingEnv),
          workerId = id
        )
        .flatMapPar(Int.MaxValue) {
          case (shardID, shardStream, checkpointer) =>
            shardStream
              .tap(r =>
                checkpointer
                  .stageOnSuccess(putStrLn(s"Processing record $r").when(false))(r)
              )
              .aggregateAsyncWithin(ZTransducer.last, Schedule.fixed(5.second))
              .mapConcat(_.toList)
              .mapError[Either[Throwable, ShardLeaseLost.type]](Left(_))
              .tap(_ => checkpointer.checkpoint)
              .catchAll {
                case Right(ShardLeaseLost) =>
                  ZStream.empty
                case Left(e)               =>
                  ZStream.fromEffect(log.error(s"${id} got" + e)) *> ZStream.fail(e)
              }
        }

    for {
      _        <- TestUtil.createStreamUnmanaged(streamName, nrShards)
      producer <- produceRecords(streamName, nrRecords).fork
      nrWorkers = 4
      workers  <- ZIO.foreachPar_(1 to nrWorkers)(id => worker(s"worker${id}").runCollect).fork
      _        <- ZIO.raceAll(ZIO.sleep(2.minute), List(workers.join, producer.join))
      _         = println("Interrupting app")
      _        <- producer.interrupt
      _        <- workers.interrupt
    } yield ExitCode.success
  }.orDie.provideCustomLayer(
    awsEnv // TODO!!
    // localStackEnv
  )

  // Based on AWS profile
  val kinesisAsyncClientProfile: ZLayer[Any, Throwable, Has[KinesisAsyncClient]] =
    ZLayer.fromManaged(
      ZManaged.fromAutoCloseable(ZIO.effect(KinesisAsyncClient.builder.build()))
    )

  val dynamoDbAsyncClientProfile: ZLayer[Any, Throwable, Has[DynamoDbAsyncClient]] =
    ZLayer.fromManaged(
      ZManaged.fromAutoCloseable(ZIO.effect(DynamoDbAsyncClient.builder.build()))
    )

  val loggingEnv = Slf4jLogger.make((_, logEntry) => logEntry, Some(getClass().getName))

  val localStackEnv =
    (Layers.kinesisAsyncClient >>> (Layers.adminClient ++ Layers.client)).orDie ++ Layers.dynamo.orDie ++ loggingEnv

  val awsEnv =
    (kinesisAsyncClientProfile >>> (Layers.adminClient ++ Layers.client)).orDie ++ dynamoDbAsyncClientProfile.orDie ++ loggingEnv

  def produceRecords(streamName: String, nrRecords: Int) =
    (for {
      client   <- ZIO.service[Client].toManaged_
      producer <- Producer.make(streamName, client, Serde.asciiString)
    } yield producer).use { producer =>
      ZStream
        .range(1, nrRecords)
        .map(i => ProducerRecord(s"key$i", s"msg$i"))
        .chunkN(499)
        .mapChunksM(
          producer
            .produceChunk(_)
            .tapError(e => putStrLn(s"error: $e").provideLayer(Console.live))
            .retry(retryOnResourceNotFound && Schedule.recurs(1))
            .as(Chunk.unit)
          // .delay(1.second)
        )
        .runDrain
    }
}
