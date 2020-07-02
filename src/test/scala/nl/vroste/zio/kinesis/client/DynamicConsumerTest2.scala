package nl.vroste.zio.kinesis.client

import java.util.UUID

import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.serde.Serde
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console._
import zio.duration._
import zio.stream.{ ZStream, ZTransducer }
import zio.test.TestAspect._
import zio.test._

object DynamicConsumerTest2 extends DefaultRunnableSpec {
  import TestUtil._

  private val env: ZLayer[Any, Throwable, Client with AdminClient with DynamicConsumer with Clock] =
    (LocalStackServices.kinesisAsyncClientLayer >>> (Client.live ++ AdminClient.live ++ LocalStackServices.dynamicConsumerLayer)) ++ Clock.live

  def testCheckpointAtShutdown =
    testM("checkpoint for the last processed record at stream shutdown") {
      val streamName      = "zio-test-stream-" + UUID.randomUUID().toString
      val applicationName = "zio-test-" + UUID.randomUUID().toString

      val batchSize = 100
      val nrBatches = 100
//      val records   =
//        (1 to batchSize).map(i => ProducerRecord(s"key$i", s"msg$i"))

      def streamConsumer(
        interrupted: Promise[Nothing, Unit],
        lastProcessedRecords: Ref[Map[String, String]],
        lastCheckpointedRecords: Ref[Map[String, String]]
      ): ZStream[Console with Blocking with Clock with DynamicConsumer, Throwable, DynamicConsumer.Record[
        String
      ]] =
        (for {
          service <- ZStream.service[DynamicConsumer.Service]
          stream  <- service
                      .shardedStream(
                        streamName,
                        applicationName = applicationName,
                        deserializer = Serde.asciiString,
                        isEnhancedFanOut = false,
                        requestShutdown = interrupted.await.tap(_ => UIO(println("Interrupting shardedStream")))
                      )
                      .flatMapPar(Int.MaxValue) {
                        case (shardId, shardStream, checkpointer @ _) =>
                          shardStream
                            .tap(record => lastProcessedRecords.update(_ + (shardId -> record.sequenceNumber)))
                            .tap(checkpointer.stage)
                            .tap(record =>
                              ZIO.when(record.partitionKey == "key1000")(
                                putStrLn("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
                                  *> interrupted.succeed(())
                              )
                            )
                            // .tap(r => putStrLn(s"Shard ${shardId} got record ${r.data}"))
                            // It's important that the checkpointing is always done before flattening the stream, otherwise
                            // we cannot guarantee that the KCL has not yet shutdown the record processor and taken away the lease
                            .aggregateAsyncWithin(
                              ZTransducer.last, // TODO we need to make sure that in our test this thing has some records buffered after shutdown request
                              Schedule.fixed(1.seconds)
                            )
                            .mapConcat(_.toList)
                            .tap { r =>
                              putStrLn(s"Shard ${r.shardId}: checkpointing for record $r $interrupted") *>
                                checkpointer.checkpoint
                                  .tapError(e => ZIO(println(s"Checkpointing failed: ${e}")))
                                  .tap(_ => lastCheckpointedRecords.update(_ + (shardId -> r.sequenceNumber)))
                                  .tap(_ => ZIO(println(s"Checkpointing for shard ${r.shardId} done")))
                            }
                      }
        } yield stream)

      createStream(streamName, 2)
        .provideSomeLayer[Console](env)
        .use { _ =>
          for {
            producing                 <- ZStream
                           .fromIterable(1 to nrBatches)
                           .schedule(Schedule.spaced(250.millis))
                           .mapM { batchIndex =>
                             ZIO
                               .accessM[Client](
                                 _.get
                                   .putRecords(
                                     streamName,
                                     Serde.asciiString,
                                     recordsForBatch(batchIndex, batchSize).map(i => ProducerRecord(s"key$i", s"msg$i"))
                                   )
                               )
                               //                             .tap(_ => putStrLn("Put records on stream"))
                               .tapError(e => putStrLn(s"error: $e"))
                               .retry(retryOnResourceNotFound)
                           }
                           .runDrain
                           .tap(_ => ZIO(println("PRODUCING RECORDS DONE")))
                           .forkAs("RecordProducing")

            interrupted               <- Promise
                             .make[Nothing, Unit]
//                             .tap(p => (putStrLn("INTERRUPTING") *> p.succeed(())).delay(19.seconds + 333.millis).fork)
            lastProcessedRecords      <- Ref.make[Map[String, String]](Map.empty) // Shard -> Sequence Nr
            lastCheckpointedRecords   <- Ref.make[Map[String, String]](Map.empty) // Shard -> Sequence Nr
            _                         <- putStrLn("ABOUT TO START CONSUMER")
            consumer                  <- streamConsumer(interrupted, lastProcessedRecords, lastCheckpointedRecords).runCollect.fork
//            _                         <- ZIO.unit.delay(30.seconds)
            _                         <- interrupted.await
            _                         <- producing.interrupt
            _                         <- consumer.join
            (processed, checkpointed) <- (lastProcessedRecords.get zip lastCheckpointedRecords.get)
          } yield assert(processed)(Assertion.equalTo(checkpointed))
        }
    } @@ TestAspect.timeout(60.seconds)

  // TODO check the order of received records is correct

  override def spec =
    suite("DynamicConsumer2")(
      testCheckpointAtShutdown
    ).provideCustomLayer(env.orDie) @@ timeout(5.minute) @@ sequential

  def delayStream[R, E, O](s: ZStream[R, E, O], delay: Duration) =
    ZStream.fromEffect(ZIO.sleep(delay)).flatMap(_ => s)

  def awaitRefPredicate[T](ref: Ref[T])(predicate: T => Boolean) =
    (for {
      p <- Promise.make[Nothing, Unit]
      _ <- ZIO
             .whenM(ref.get.map(predicate))(p.succeed(()))
             .repeat(Schedule.fixed(1.second))
             .fork
      _ <- p.await
    } yield ()).fork

}
