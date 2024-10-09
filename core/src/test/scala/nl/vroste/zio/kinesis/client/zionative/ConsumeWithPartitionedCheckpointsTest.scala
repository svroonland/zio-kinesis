package nl.vroste.zio.kinesis.client.zionative

import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.{ ProducerRecord, TestUtil }
import zio.test.Assertion.{ equalTo, isSome }
import zio.test.TestAspect.{ timeout, withLiveClock, withLiveRandom }
import zio.test.{ assert, ZIOSpecDefault }
import zio._
import TestUtil._
import nl.vroste.zio.kinesis.client.FakeRecordProcessor
import nl.vroste.zio.kinesis.client.zionative.leaserepository.DynamoDbLeaseRepository

object ConsumeWithPartitionedCheckpointsTest extends ZIOSpecDefault {
  override def spec =
    suite("ConsumeWithPartitionedCheckpoints")(
      test("consumePartitionedWith should consume records produced on all shards") {
        withRandomStreamEnv(2) { (streamName, applicationName) =>
          val nrRecords = 4

          ZIO.scoped {
            for {
              refProcessed      <- Ref.make(Seq.empty[String])
              finishedConsuming <- Promise.make[Nothing, Unit]
              records            = (1 to nrRecords).map(i => ProducerRecord(s"key$i", s"msg$i"))
              _                 <- putRecords(streamName, Serde.asciiString, records).retry(retryOnResourceNotFound)
              processor          = FakeRecordProcessor.make[String](
                                     refProcessed,
                                     finishedConsuming,
                                     expectedCount = nrRecords
                                   )
              _                 <- Consumer.consumeWith(
                                     streamName,
                                     applicationName = applicationName,
                                     deserializer = Serde.asciiString,
                                     consumptionBehaviour = ConsumptionBehaviour.partitionedCheckpoints(checkpointBatchSize = 2)
                                   )(r => processor(r.data)) raceFirst finishedConsuming.await
              processedRecords  <- refProcessed.get
            } yield assert(processedRecords.distinct.size)(equalTo(nrRecords))
          }
        }
      },
      test(
        "consumePartitionedWith should, after a restart due to a record processing error, consume records produced on all shards"
      ) {
        withRandomStreamEnv(2) { (streamName, applicationName) =>
          val nrRecords = 50
          val batchSize = 10L

          ZIO.scoped {
            for {
              refProcessed      <- Ref.make(Seq.empty[String])
              finishedConsuming <- Promise.make[Nothing, Unit]
              records            = (1 to nrRecords).map(i => ProducerRecord(s"key$i", s"msg$i"))
              _                 <- putRecords(streamName, Serde.asciiString, records).retry(retryOnResourceNotFound)
              prosessor          = FakeRecordProcessor.makeFailing(
                                     refProcessed,
                                     finishedConsuming,
                                     failFunction = (_: Any) == "msg31"
                                   )
              _                 <- Consumer
                                     .consumeWith(
                                       streamName,
                                       applicationName = applicationName,
                                       deserializer = Serde.asciiString,
                                       consumptionBehaviour =
                                         ConsumptionBehaviour.partitionedCheckpoints(checkpointBatchSize = batchSize)
                                     )(r => prosessor(r.data))
                                     .ignore
              _                  = println("Starting dynamic consumer - about to succeed")
              processor2         = FakeRecordProcessor.make[String](
                                     refProcessed,
                                     finishedConsuming,
                                     expectedCount = nrRecords
                                   )
              _                 <- Consumer.consumeWith(
                                     streamName,
                                     applicationName = applicationName,
                                     deserializer = Serde.asciiString,
                                     consumptionBehaviour = ConsumptionBehaviour.partitionedCheckpoints(checkpointBatchSize = batchSize)
                                   )(r => processor2(r.data)) raceFirst finishedConsuming.await
              processedRecords  <- refProcessed.get
            } yield assert(processedRecords.distinct.size)(equalTo(nrRecords))
          }
        }
      },
      test(
        "consumePartitionedWith should checkpoint before consuming the second record with the same key"
      ) {
        withRandomStreamEnv(1) { (streamName, applicationName) =>
          val records = List(
            ProducerRecord("key1", "msg1"),
            ProducerRecord("key2", "msg2"),
            ProducerRecord("key1", "msg3")
          )

          val getSequenceNumber = ZIO.serviceWithZIO[LeaseRepository](
            _.getLeases(applicationName)
              // we only have one shard, take the first lease we find
              .runHead
              .map(_.flatMap(_.checkpoint.flatMap(_.toOption)))
          )

          ZIO.scoped {
            for {
              refProcessed <- Ref.make(Seq.empty[String])
              latch1       <- Promise.make[Nothing, Unit]
              latch2       <- Promise.make[Nothing, Unit]
              latch3       <- Promise.make[Nothing, Unit]
              _            <- putRecords(streamName, Serde.asciiString, records).retry(retryOnResourceNotFound)
              prosessor     = (data: String) =>
                                for {
                                  _ <- refProcessed.update(_ :+ data)
                                  _ <- (latch1.succeed(()) *> latch2.await).when(data == "msg2")
                                  _ <- latch3.succeed(()).when(data == "msg3")
                                } yield ()
              consumer     <- (Consumer
                                .consumeWith(
                                  streamName,
                                  applicationName = applicationName,
                                  deserializer = Serde.asciiString,
                                  consumptionBehaviour = ConsumptionBehaviour.partitionedCheckpoints(
                                    checkpointBatchSize =
                                      1000, // ensure we will not checkpoint due to batch size or timeout
                                    checkpointDuration = Duration.Infinity
                                  )
                                )(r => prosessor(r.data))
                                .ignore raceFirst latch3.await).forkScoped

              _          <- latch1.await
              // wait a bit to ensure no more records are processed
              _          <- ZIO.sleep(100.millis)
              processed1 <- refProcessed.get
              checkpoint <- getSequenceNumber.repeatUntil(_.isDefined)

              _ <- latch2.succeed(())
              _ <- consumer.join

              processed2 <- refProcessed.get

            } yield assert(processed1.size)(equalTo(2)) &&
              assert(processed2.size)(equalTo(3)) &&
              assert(checkpoint)(isSome)
          }
        }
      }
    )
      .@@(withLiveClock)
      .@@(timeout(7.minutes))
      .@@(withLiveRandom)
      .provideShared(awsLayer, DynamoDbLeaseRepository.live)
}
