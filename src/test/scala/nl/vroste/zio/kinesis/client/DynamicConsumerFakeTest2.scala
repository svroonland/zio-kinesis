package nl.vroste.zio.kinesis.client

import java.nio.ByteBuffer
import java.time.OffsetDateTime

import nl.vroste.zio.kinesis.client.serde.Serde
import software.amazon.awssdk.services.kinesis.model.EncryptionType
import zio.console.putStrLn
import zio.logging.Logging
import zio.stream.{ ZStream, ZTransducer }
import zio.test._
import zio.{ Queue, Ref, Schedule, ZIO }
import zio.duration._

object DynamicConsumerFakeTest2 extends DefaultRunnableSpec {
  private type Shard = ZStream[Any, Nothing, (String, ZStream[Any, Throwable, ByteBuffer])]

  private val loggingLayer = Logging.console() >>> Logging.withRootLoggerName(getClass.getName)

  private val shardsFromIterables: Shard        =
    DynamicConsumerFake.shardsFromIterables(Serde.asciiString, List("msg0", "msg1"), List("msg2", "msg3"))
  private val shardsFromStreams: Shard          =
    DynamicConsumerFake.shardsFromStreams(Serde.asciiString, ZStream("msg0", "msg1"), ZStream("msg2", "msg3"))
  private val singleShardWithFourRecords: Shard =
    DynamicConsumerFake.shardsFromIterables(Serde.asciiString, List("msg0", "msg1", "msg2", "msg3"))

  private val now = OffsetDateTime.parse("1970-01-01T00:00:00Z")

  private def record(sequenceNumber: Long, shardName: String, data: String) =
    Record[String](
      sequenceNumber = s"$sequenceNumber",
      approximateArrivalTimestamp = now.toInstant,
      data = data,
      partitionKey = s"${shardName}_$sequenceNumber",
      encryptionType = EncryptionType.NONE,
      subSequenceNumber = Some(sequenceNumber),
      explicitHashKey = None,
      aggregated = false,
      shardId = shardName
    )

  def program(shards: Shard, checkpointBatchSize: Int) =
    for {
      refCheckpointedList <- Ref.make[Seq[Any]](Seq.empty[String])
      _                   <- DynamicConsumer
             .shardedStream(
               streamName = "my-stream",
               applicationName = "my-application",
               deserializer = Serde.asciiString,
               workerIdentifier = "worker1"
             )
             .flatMapPar(Int.MaxValue) {
               case (shardId, shardStream, checkpointer) =>
                 shardStream
                   .tap(record => putStrLn(s"Processing record ${record} on shard ${shardId}"))
                   .tap(checkpointer.stage(_))
                   .tap { record =>
                     println(s"XXXXXXXXXXXXXXXXXX record.subSequenceNumber.getOrElse[Long](0) ${record.subSequenceNumber
                       .getOrElse[Long](0)}")
                     ZIO.when(record.subSequenceNumber.getOrElse[Long](0) % 2 != 0)(checkpointer.checkpoint)
                   }
             }
             .runDrain
             .provideCustomLayer(DynamicConsumer.fake(shards, refCheckpointedList) ++ loggingLayer)
             .exitCode
      checkpointedList    <- refCheckpointedList.get
    } yield checkpointedList

  override def spec =
    suite("DynamicConsumerFake should")(
      testM("checkpointer.checkpointBatched should batch every 3 records when batch size is 2") {
        for {
          checkpointedList <- program(singleShardWithFourRecords, checkpointBatchSize = 1)
        } yield assert(checkpointedList)(
          Assertion.hasSameElementsDistinct(
            List(
              record(sequenceNumber = 2, shardName = "shard0", data = "msg2"),
              record(sequenceNumber = 5, shardName = "shard0", data = "msg5")
            )
          )
        )
      } @@ TestAspect.ignore,
      testM("aggregateAsyncWithin should batch single records when max is 1, when using unbounded Q") {
        val stream = ZStream(1, 2, 3)

        for {
          q1   <- Queue.unbounded[Int]
          q2   <- Queue.unbounded[Option[Int]]
          _    <- stream
                 .tap(i => q1.offer(i))
                 .aggregateAsyncWithin(ZTransducer.foldUntil((), 1)((_, _) => ()), Schedule.fixed(1.minute))
                 .tap(_ => q1.poll.flatMap(o => q2.offer(o)))
                 .runCollect
          size <- q2.size
          xs   <- q2.takeAll
          _    <- putStrLn(s"q2=${xs}")
        } yield assert(size)(Assertion.equalTo(3))
      } @@ TestAspect.ignore,
      testM("aggregateAsyncWithin should batch single records when max is 1, when using a Ref") {
        val stream = ZStream(1, 2, 3)

        for {
          refLatest     <- Ref.make[Option[Int]](None)
          checkpointedQ <- Queue.unbounded[Option[Int]]
          _             <- stream
                 .tap(i => putStrLn(s"processing $i") *> refLatest.modify(_ => (i, Some(i)))) // Stage record
                 .aggregateAsyncWithin(ZTransducer.foldUntil((), 1)((_, _) => ()), Schedule.fixed(1.minute))
                 .tap(_ => refLatest.get.flatMap(o => checkpointedQ.offer(o)))                // checkpoint
                 .runCollect
          size          <- checkpointedQ.size
          xs            <- checkpointedQ.takeAll
          _             <- putStrLn(s"q=${xs}")
        } yield assert(xs)(
          Assertion.equalTo(List(Some(2), Some(3), Some(3)))
        ) // wrongly, I expected List(Some(1), Some(2), Some(3))
      },
      testM("aggregateAsyncWithin should batch single records when max is 1, when using dropping Q") {
        val stream = ZStream(1, 2, 3)

        for {
          q1   <- Queue.dropping[Int](1)
          q2   <- Queue.unbounded[Int]
          _    <- stream
                 .tap(i => q1.offer(i))
                 .aggregateAsyncWithin(ZTransducer.foldUntil((), 1)((_, _) => ()), Schedule.fixed(1.minute))
                 .tap(_ => q1.take.flatMap(o => q2.offer(o)))
                 .runCollect
          size <- q2.size
          xs   <- q2.takeAll
          _    <- putStrLn(s"q2=${xs}")
        } yield assert(size)(Assertion.equalTo(3))
      } @@ TestAspect.ignore
    )
}
