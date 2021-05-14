package nl.vroste.zio.kinesis.client
import io.github.vigoo.zioaws.kinesis.model.PutRecordsRequestEntry
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.zionative.protobuf.Messages
import zio.ZIO
import zio.test.Assertion._
import zio.test._

object ProtobufAggregationTest extends DefaultRunnableSpec {
  override def spec =
    suite("ProtobufAggregation")(
      testM("roundtrip encoding") {
        val payload      = "this is the payload"
        val partitionKey = "key1"

        for {
          bytes         <- Serde.asciiString.serialize(payload)
          entry          = PutRecordsRequestEntry(bytes, partitionKey = "123")
          protobufRecord = ProtobufAggregation.putRecordsRequestEntryToRecord(entry.data, None, 0)

          aggregatedRecord = Messages.AggregatedRecord
                               .newBuilder()
                               .addRecords(protobufRecord)
                               .addPartitionKeyTable(partitionKey)
                               .addExplicitHashKeyTable(partitionKey)
                               .build()
          encoded          = ProtobufAggregation.encodeAggregatedRecord(aggregatedRecord)

          decoded <- ZIO.fromTry(ProtobufAggregation.decodeAggregatedRecord(encoded))
        } yield assert(decoded.getRecords(0).getData.toByteArray)(equalTo(bytes.toArray))

      }
    )
}
