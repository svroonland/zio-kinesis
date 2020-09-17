package nl.vroste.zio.kinesis.client

import java.nio.ByteBuffer
import java.time.OffsetDateTime

import nl.vroste.zio.kinesis.client.serde.Serde
import software.amazon.awssdk.services.kinesis.model.EncryptionType
import zio.Ref
import zio.console.putStrLn
import zio.duration._
import zio.logging.Logging
import zio.stream.ZStream
import zio.test._

object DynamicConsumerFakeTestOld extends DefaultRunnableSpec {
  private type Shard = ZStream[Any, Nothing, (String, ZStream[Any, Throwable, ByteBuffer])]

  private val loggingLayer = Logging.console() >>> Logging.withRootLoggerName(getClass.getName)

  private val shardsFromIterables: Shard       =
    DynamicConsumerFake.shardsFromIterables(Serde.asciiString, List("msg1", "msg2"), List("msg3", "msg4"))
  private val shardsFromStreams: Shard         =
    DynamicConsumerFake.shardsFromStreams(Serde.asciiString, ZStream("msg1", "msg2"), ZStream("msg3", "msg4"))
  private val singleShardWithTwoRecords: Shard =
    DynamicConsumerFake.shardsFromIterables(Serde.asciiString, List("msg0", "msg1"))

  private val now = OffsetDateTime.parse("1970-01-01T00:00:00Z")

  private def record(sequenceNumber: Long, shardName: String, data: String) =
    Record[String](
      sequenceNumber = s"$sequenceNumber",
      approximateArrivalTimestamp = now.toInstant,
      data = data,
      partitionKey = s"${shardName}_$sequenceNumber",
      encryptionType = EncryptionType.NONE,
      subSequenceNumber = None,
      explicitHashKey = None,
      aggregated = false,
      shardId = shardName
    )

  def program(shards: Shard, checkpointBatchSize: Long = 1000L) =
    for {
      refCheckpointedList <- Ref.make[Seq[Any]](Seq.empty[String])
      _                   <- DynamicConsumer
             .consumeWith(
               streamName = "my-stream",
               applicationName = "my-application",
               deserializer = Serde.asciiString,
               workerIdentifier = "worker1",
               checkpointBatchSize = checkpointBatchSize,
               checkpointDuration = 5.minutes
             )(record => putStrLn(s"Processing record $record"))
             .provideCustomLayer(DynamicConsumer.fake(shards, refCheckpointedList) ++ loggingLayer)
             .exitCode
      checkpointedList    <- refCheckpointedList.get
    } yield checkpointedList

  override def spec =
    suite("DynamicConsumerFake should")(
      testM("read from iterables when using shardsFromIterables") {
        for {
          checkpointedList <- program(shardsFromIterables)
        } yield assert(checkpointedList)(
          Assertion.hasSameElementsDistinct(
            List(
              record(sequenceNumber = 1, shardName = "shard0", data = "msg2"),
              record(sequenceNumber = 1, shardName = "shard1", data = "msg4")
            )
          )
        )
      } @@ TestAspect.ignore,
      testM("read from streams when using shardsFromStreams") {
        for {
          checkpointedList <- program(shardsFromStreams)
        } yield assert(checkpointedList)(
          Assertion.hasSameElementsDistinct(
            List(
              record(sequenceNumber = 1, shardName = "shard0", data = "msg2"),
              record(sequenceNumber = 1, shardName = "shard1", data = "msg4")
            )
          )
        )
      } @@ TestAspect.ignore,
      testM("checkpointer.checkpointBatched should batch every 3 records when batch size is 2") {
        for {
          checkpointedList <- program(singleShardWithTwoRecords, checkpointBatchSize = 1)
        } yield assert(checkpointedList)(
          Assertion.hasSameElementsDistinct(
            List(
              record(sequenceNumber = 2, shardName = "shard0", data = "msg2"),
              record(sequenceNumber = 5, shardName = "shard0", data = "msg5")
            )
          )
        )
      }
    )
}
