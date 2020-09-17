package nl.vroste.zio.kinesis.client
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.zionative.protobuf.Messages
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry
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
          entry          = PutRecordsRequestEntry.builder().data(SdkBytes.fromByteBuffer(bytes)).build()
          protobufRecord = ProtobufAggregation.putRecordsRequestEntryToRecord(entry, 0)

          aggregatedRecord = Messages.AggregatedRecord
                               .newBuilder()
                               .addRecords(protobufRecord)
                               .addPartitionKeyTable(partitionKey)
                               .addExplicitHashKeyTable(partitionKey)
                               .build()
          encoded          = ProtobufAggregation.encodeAggregatedRecord(aggregatedRecord)

          decoded <- ZIO.fromTry(ProtobufAggregation.decodeAggregatedRecord(encoded))
        } yield assert(decoded.getRecords(0).getData.asReadOnlyByteBuffer())(equalTo(bytes))

      }
    )
}
