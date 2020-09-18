package nl.vroste.zio.kinesis.client.zionative
import java.time.Instant

import nl.vroste.zio.kinesis.client.Record
import nl.vroste.zio.kinesis.client.zionative.leasecoordinator.DefaultCheckpointer
import nl.vroste.zio.kinesis.client.zionative.leasecoordinator.DefaultCheckpointer.UpdateCheckpoint
import software.amazon.awssdk.services.kinesis.model.EncryptionType
import zio.logging.Logging
import zio.{ Promise, Ref, Semaphore, Task, ZIO }
import zio.test._
import zio.test.Assertion._

object DefaultCheckpointerTest extends DefaultRunnableSpec {
  val record1 = Record("shard1", "0", Instant.now, "bla", "bla", EncryptionType.NONE, None, None, false)
  val record2 = Record("shard1", "1", Instant.now, "bla", "bla", EncryptionType.NONE, None, None, false)

  override def spec =
    suite("DefaultCheckpointer")(
      testM("checkpoints the last staged record") {
        for {
          checkpoints  <- Ref.make(List.empty[Either[SpecialCheckpoint, ExtendedSequenceNumber]])
          checkpointer <- {
            val updateCheckpoint: UpdateCheckpoint = {
              case (seqNr, _, _) => checkpoints.update(_ :+ seqNr)
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
          checkpoints  <- Ref.make(List.empty[Either[SpecialCheckpoint, ExtendedSequenceNumber]])
          checkpointer <- {
            val updateCheckpoint: UpdateCheckpoint = {
              case (seqNr, _, _) => checkpoints.update(_ :+ seqNr)
            }
            makeCheckpointer(updateCheckpoint)
          }
          _            <- checkpointer.stage(record1)
          _            <- checkpointer.checkpoint()
          _            <- checkpointer.checkpoint()
          values       <- checkpoints.get
        } yield assert(values.map(_.toOption.get.sequenceNumber))(equalTo(List("0")))
      },
      testM("preserves the last staged checkpoint while checkpointing") {
        for {
          checkpoints  <- Ref.make(List.empty[Either[SpecialCheckpoint, ExtendedSequenceNumber]])
          latch1       <- Promise.make[Nothing, Unit]
          latch2       <- Promise.make[Nothing, Unit]
          checkpointer <- {
            val updateCheckpoint: UpdateCheckpoint = {
              case (seqNr, _, _) => latch1.succeed(()) *> latch2.await *> checkpoints.update(_ :+ seqNr)
            }
            makeCheckpointer(updateCheckpoint)
          }
          _            <- checkpointer.stage(record1)
          _            <- checkpointer.checkpoint() <&
                 (latch1.await *> checkpointer.stage(record2) *> latch2.succeed(()))
          _            <- checkpointer.checkpoint()
          values       <- checkpoints.get
        } yield assert(values.map(_.toOption.get.sequenceNumber))(equalTo(List("0", "1")))
      }
    ).provideCustomLayerShared(Logging.ignore)

  /*
  TODO:
  - retries according to schedule
  - releases
  - not beyond max seq nr (also implement this)
  - handles shard end correctly (TBD)
   */

  private def makeCheckpointer(updateCheckpoint: UpdateCheckpoint): ZIO[Logging, Nothing, DefaultCheckpointer] =
    for {
      state       <- Ref.make(DefaultCheckpointer.State.empty)
      permit      <- Semaphore.make(1)
      env         <- ZIO.environment[Logging]
      checkpointer = new DefaultCheckpointer("shard1", env, state, permit, updateCheckpoint, Task.unit)
    } yield checkpointer
}
