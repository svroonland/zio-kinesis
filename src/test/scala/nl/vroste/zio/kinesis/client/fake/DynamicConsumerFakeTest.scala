package nl.vroste.zio.kinesis.client.fake

import java.nio.ByteBuffer
import java.time.OffsetDateTime

import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.DynamicConsumer
import nl.vroste.zio.kinesis.client.DynamicConsumer.Record
import software.amazon.awssdk.services.kinesis.model.EncryptionType
import zio.clock.Clock
import zio.console.Console
import zio.duration._
import zio.logging.Logging
import zio.stream.ZStream
import zio.test._
import zio.{ Queue, Ref, ZLayer }

object DynamicConsumerFakeTest extends DefaultRunnableSpec {
  private type Shard = ZStream[Any, Nothing, (String, ZStream[Any, Throwable, ByteBuffer])]

  private val now = OffsetDateTime.parse("1970-01-01T00:00:00Z")

  private val loggingLayer: ZLayer[Any, Nothing, Logging] =
    (Console.live ++ Clock.live) >>> Logging.console() >>> Logging.withRootLoggerName(getClass.getName)

  private val shardsFromIterables: Shard =
    DynamicConsumerFake.shardsFromIterables(Serde.asciiString, List("msg1", "msg2"), List("msg3", "msg4"))
  private val shardsFromStreams: Shard   =
    DynamicConsumerFake.shardsFromStreams(Serde.asciiString, ZStream("msg1", "msg2"), ZStream("msg3", "msg4"))

  private val expectedRecords = {
    def recordsForShard(shardName: String, xs: String*) =
      xs.zipWithIndex.map {
        case (s, i) =>
          record(i.toLong, shardName, s)
      }
    recordsForShard("shard0", "msg1", "msg2") ++ recordsForShard("shard1", "msg3", "msg4")
  }

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

  def programCheckpointed(shards: Shard) =
    for {
      q                   <- Queue.unbounded[Record[String]]
      refCheckpointedList <- Ref.make[Seq[_]](Seq.empty[String])
      _                   <- DynamicConsumer
             .consumeWith(
               streamName = "my-stream",
               applicationName = "my-application",
               deserializer = Serde.asciiString,
               workerIdentifier = "worker1",
               checkpointBatchSize = 1000L,
               checkpointDuration = 5.minutes
             )(record => q.offer(record).unit)
             .provideCustomLayer(DynamicConsumer.fake(shards, refCheckpointedList) ++ loggingLayer)
             .exitCode
      checkpointedList    <- refCheckpointedList.get
      xs                  <- q.takeAll
    } yield (checkpointedList, xs)

  def program(shards: Shard) =
    for {
      q  <- Queue.unbounded[Record[String]]
      _  <- DynamicConsumer
             .consumeWith(
               streamName = "my-stream",
               applicationName = "my-application",
               deserializer = Serde.asciiString,
               workerIdentifier = "worker1",
               checkpointBatchSize = 1000L,
               checkpointDuration = 5.minutes
             )(record => q.offer(record).unit)
             .provideCustomLayer(DynamicConsumer.fake(shards) ++ loggingLayer)
             .exitCode
      xs <- q.takeAll
    } yield xs

  override def spec =
    suite("DynamicConsumerFake should")(
      suite("when checkpointed")(
        testM("read from iterables when using shardsFromIterables") {
          for {
            t <- programCheckpointed(shardsFromIterables)
          } yield assert(t._1)(
            Assertion.hasSameElementsDistinct(
              List(
                record(sequenceNumber = 1, shardName = "shard0", data = "msg2"),
                record(sequenceNumber = 1, shardName = "shard1", data = "msg4")
              )
            )
          ) && assert(t._2)(Assertion.hasSameElementsDistinct(expectedRecords))
        },
        testM("read from streams when using shardsFromStreams") {
          for {
            t <- programCheckpointed(shardsFromStreams)
          } yield assert(t._1)(
            Assertion.hasSameElementsDistinct(
              List(
                record(sequenceNumber = 1, shardName = "shard0", data = "msg2"),
                record(sequenceNumber = 1, shardName = "shard1", data = "msg4")
              )
            )
          ) && assert(t._2)(Assertion.hasSameElementsDistinct(expectedRecords))
        }
      ),
      suite("when not checkpointed")(
        testM("read from iterables when using shardsFromIterables") {
          for {
            xs <- program(shardsFromIterables)
          } yield assert(xs)(Assertion.hasSameElementsDistinct(expectedRecords))
        },
        testM("read from streams when using shardsFromStreams") {
          for {
            xs <- program(shardsFromStreams)
          } yield assert(xs)(Assertion.hasSameElementsDistinct(expectedRecords))
        }
      )
    )
}
