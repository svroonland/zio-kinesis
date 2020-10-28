package nl.vroste.zio.kinesis.client.zionative
import java.time.Instant

import nl.vroste.zio.kinesis.client.Record
import nl.vroste.zio.kinesis.client.zionative.leasecoordinator.DefaultCheckpointer
import nl.vroste.zio.kinesis.client.zionative.leasecoordinator.DefaultCheckpointer.UpdateCheckpoint
<<<<<<< HEAD
=======
import software.amazon.awssdk.services.kinesis.model.EncryptionType
>>>>>>> origin/master
import zio.logging.Logging
import zio.{ Promise, Ref, Schedule, Semaphore, Task, ZIO }
import zio.test._
import zio.test.Assertion._

import scala.concurrent.TimeoutException

object DefaultCheckpointerTest extends DefaultRunnableSpec {
  type Checkpoint = Either[SpecialCheckpoint, ExtendedSequenceNumber]
<<<<<<< HEAD
  val record1 = Record("shard1", "0", Instant.now, "bla", "bla", None, None, None, false)
  val record2 = Record("shard1", "1", Instant.now, "bla", "bla", None, None, None, false)
=======
  val record1 = Record("shard1", "0", Instant.now, "bla", "bla", EncryptionType.NONE, None, None, false)
  val record2 = Record("shard1", "1", Instant.now, "bla", "bla", EncryptionType.NONE, None, None, false)
>>>>>>> origin/master

  override def spec =
    suite("DefaultCheckpointer")(
      testM("checkpoints the last staged record") {
        for {
          checkpoints  <- Ref.make(List.empty[Checkpoint])
          checkpointer <- {
            val updateCheckpoint: UpdateCheckpoint = {
              case (seqNr, _) => checkpoints.update(_ :+ seqNr)
            }
            makeCheckpointer(updateCheckpoint)
          }
          _            <- checkpointer.stage(record1)
          _            <- checkpointer.stage(record2)
          _            <- checkpointer.checkpoint()
          values       <- checkpoints.get
        } yield assert(values.map(_.toOption.get.sequenceNumber))(equalTo(List("1")))
      },
      testM("does not checkpoint the same staged checkpoint twice") {
        for {
          checkpoints  <- Ref.make(List.empty[Checkpoint])
          checkpointer <- {
            val updateCheckpoint: UpdateCheckpoint = {
              case (seqNr, _) => checkpoints.update(_ :+ seqNr)
            }
            makeCheckpointer(updateCheckpoint)
          }
          _            <- checkpointer.stage(record1)
          _            <- checkpointer.checkpoint()
          _            <- checkpointer.checkpoint()
          values       <- checkpoints.get
        } yield assert(values.map(_.toOption.get.sequenceNumber))(equalTo(List("0")))
      },
      testM("does not reset the last staged checkpoitn when checkpointing fails") {
        for {
          checkpoints       <- Ref.make(List.empty[Checkpoint])
          checkpointAttempt <- Ref.make(0)
          checkpointer      <- {
            val updateCheckpoint: UpdateCheckpoint = {
              case (seqNr, _) =>
                checkpointAttempt.getAndUpdate(_ + 1).flatMap { attempt =>
                  if (attempt == 0)
                    ZIO.fail(Left(new TimeoutException("Checkpoint failed")))
                  else
                    checkpoints.update(_ :+ seqNr)
                }
            }
            makeCheckpointer(updateCheckpoint)
          }
          _                 <- checkpointer.stage(record1)
          _                 <- checkpointer.checkpoint(Schedule.stop).flip
          _                 <- checkpointer.checkpoint()
          values            <- checkpoints.get
        } yield assert(values.map(_.toOption.get.sequenceNumber))(equalTo(List("0")))
      },
      testM("preserves the last staged checkpoint while checkpointing") {
        for {
          checkpoints  <- Ref.make(List.empty[Checkpoint])
          latch1       <- Promise.make[Nothing, Unit]
          latch2       <- Promise.make[Nothing, Unit]
          checkpointer <- {
            val updateCheckpoint: UpdateCheckpoint = {
              case (seqNr, _) => latch1.succeed(()) *> latch2.await *> checkpoints.update(_ :+ seqNr)
            }
            makeCheckpointer(updateCheckpoint)
          }
          _            <- checkpointer.stage(record1)
          _            <- checkpointer.checkpoint() <&
                 (latch1.await *> checkpointer.stage(record2) *> latch2.succeed(()))
          _            <- checkpointer.checkpoint()
          values       <- checkpoints.get
        } yield assert(values.map(_.toOption.get.sequenceNumber))(equalTo(List("0", "1")))
      },
      testM("checkpoints ShardEnd when the last sequence number is checkpointed after seeing the shard's end") {
        for {
          checkpoints  <- Ref.make(List.empty[Checkpoint])
          checkpointer <- {
            val updateCheckpoint: UpdateCheckpoint = {
              case (seqNr, _) => checkpoints.update(_ :+ seqNr)
            }
            makeCheckpointer(updateCheckpoint)
          }
          _            <- checkpointer.setMaxSequenceNumber(ExtendedSequenceNumber(record2.sequenceNumber, 0))
          _            <- checkpointer.markEndOfShard()
          _            <- checkpointer.stage(record1)
          _            <- checkpointer.stage(record2)
          _            <- checkpointer.checkpoint()
          values       <- checkpoints.get
        } yield assert(values)(equalTo(List(Left(SpecialCheckpoint.ShardEnd))))
      },
      testM("checkpoints ShardEnd after the last sequence number is checkpointed when seeing the shard's end") {
        for {
          checkpoints  <- Ref.make(List.empty[Checkpoint])
          checkpointer <- {
            val updateCheckpoint: UpdateCheckpoint = {
              case (seqNr, _) => checkpoints.update(_ :+ seqNr)
            }
            makeCheckpointer(updateCheckpoint)
          }
          _            <- checkpointer.setMaxSequenceNumber(ExtendedSequenceNumber(record2.sequenceNumber, 0))
          _            <- checkpointer.stage(record1)
          _            <- checkpointer.stage(record2)
          _            <- checkpointer.checkpoint()
          _            <- checkpointer.markEndOfShard()
          _            <- checkpointer.checkpointAndRelease
          values       <- checkpoints.get
        } yield assert(values)(
          equalTo(List(Right(ExtendedSequenceNumber(record2.sequenceNumber, 0)), Left(SpecialCheckpoint.ShardEnd)))
        )
      },
      testM("does not checkpoint ShardEnd when the last record has not yet been staged after seeing the shard's end") {
        for {
          checkpoints  <- Ref.make(List.empty[Checkpoint])
          checkpointer <- {
            val updateCheckpoint: UpdateCheckpoint = {
              case (seqNr, _) => checkpoints.update(_ :+ seqNr)
            }
            makeCheckpointer(updateCheckpoint)
          }
          _            <- checkpointer.setMaxSequenceNumber(ExtendedSequenceNumber(record2.sequenceNumber, 0))
          _            <- checkpointer.markEndOfShard()
          _            <- checkpointer.stage(record1)
          _            <- checkpointer.checkpoint()
          values       <- checkpoints.get
        } yield assert(values)(equalTo(List(Right(ExtendedSequenceNumber(record1.sequenceNumber, 0)))))
      },
      testM("checkpoints ShardEnd on releasing when the last record is staged after seeing the shard's end") {
        for {
          checkpoints  <- Ref.make(List.empty[Checkpoint])
          checkpointer <- {
            val updateCheckpoint: UpdateCheckpoint = {
              case (seqNr, _) => checkpoints.update(_ :+ seqNr)
            }
            makeCheckpointer(updateCheckpoint)
          }
          _            <- checkpointer.setMaxSequenceNumber(ExtendedSequenceNumber(record2.sequenceNumber, 0))
          _            <- checkpointer.markEndOfShard()
          _            <- checkpointer.stage(record1)
          _            <- checkpointer.stage(record2)
          _            <- checkpointer.checkpointAndRelease
          values       <- checkpoints.get
        } yield assert(values)(equalTo(List(Left(SpecialCheckpoint.ShardEnd))))
      },
      testM("checkpoints ShardEnd on releasing after an empty poll") {
        for {
          checkpoints  <- Ref.make(List.empty[Checkpoint])
          checkpointer <- {
            val updateCheckpoint: UpdateCheckpoint = {
              case (seqNr, _) => checkpoints.update(_ :+ seqNr)
            }
            makeCheckpointer(updateCheckpoint)
          }
          // No max sequence number
          _            <- checkpointer.markEndOfShard()
          _            <- checkpointer.checkpointAndRelease
          values       <- checkpoints.get
        } yield assert(values)(equalTo(List(Left(SpecialCheckpoint.ShardEnd))))
      }
    ).provideCustomLayerShared(Logging.ignore)

  private def makeCheckpointer(updateCheckpoint: UpdateCheckpoint): ZIO[Logging, Nothing, DefaultCheckpointer] =
    for {
      state       <- Ref.make(DefaultCheckpointer.State.empty)
      permit      <- Semaphore.make(1)
      env         <- ZIO.environment[Logging]
      checkpointer = new DefaultCheckpointer("shard1", env, state, permit, updateCheckpoint, Task.unit)
    } yield checkpointer
}
