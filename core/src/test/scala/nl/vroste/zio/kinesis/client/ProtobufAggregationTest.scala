package nl.vroste.zio.kinesis.client
import zio.aws.kinesis.model.PutRecordsRequestEntry
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.zionative.protobuf.Messages
import zio.ZIO
import zio.aws.kinesis.model.primitives.{ Data, PartitionKey }
import zio.test.Assertion._
import zio.test._

import java.security.MessageDigest

object ProtobufAggregationTest extends ZIOSpecDefault {
  override def spec =
    suite("ProtobufAggregation")(
      test("roundtrip encoding") {
        val payload      = "this is the payload"
        val partitionKey = "key1"

        for {
          bytes         <- Serde.asciiString.serialize(payload)
          entry          = PutRecordsRequestEntry(Data(bytes), partitionKey = PartitionKey("123"))
          protobufRecord = ProtobufAggregation.putRecordsRequestEntryToRecord(entry.data, None, 0)

          aggregatedRecord = Messages.AggregatedRecord
                               .newBuilder()
                               .addRecords(protobufRecord)
                               .addPartitionKeyTable(partitionKey)
                               .addExplicitHashKeyTable(partitionKey)
                               .build()
          encoded          = ProtobufAggregation.encodeAggregatedRecord(MessageDigest.getInstance("MD5"), aggregatedRecord)

          decoded <- ZIO.fromTry(ProtobufAggregation.decodeAggregatedRecord(encoded))
        } yield assert(decoded.getRecords(0).getData.toByteArray)(equalTo(bytes.toArray))

      }
    )
}
