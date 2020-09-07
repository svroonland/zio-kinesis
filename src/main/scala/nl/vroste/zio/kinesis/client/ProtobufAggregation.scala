package nl.vroste.zio.kinesis.client
import com.google.protobuf.ByteString
import nl.vroste.zio.kinesis.client.zionative.protobuf.Messages
import nl.vroste.zio.kinesis.client.zionative.protobuf.Messages.AggregatedRecord
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry
import software.amazon.awssdk.utils.Md5Utils
import zio.Chunk

import scala.util.{ Failure, Try }

object ProtobufAggregation {
  val magicBytes: Array[Byte] = List(0xf3, 0x89, 0x9a, 0xc2).map(_.toByte).toArray
  val checksumSize            = 16

  def putRecordsRequestEntryToRecord(r: PutRecordsRequestEntry, tableIndex: Int): Messages.Record =
    Messages.Record
      .newBuilder()
      .setData(ByteString.copyFrom(r.data().asByteArrayUnsafe()))
      .setPartitionKeyIndex(tableIndex)
      .setExplicitHashKeyIndex(tableIndex)
      .build()

  def encodedSize(ar: AggregatedRecord): Int =
    magicBytes.length + ar.getSerializedSize + checksumSize

  def encodeAggregatedRecord(ar: AggregatedRecord): Chunk[Byte] = {
    val payload  = ar.toByteArray
    val checksum = Chunk.fromArray(Md5Utils.computeMD5Hash(payload))
    Chunk.fromArray(magicBytes) ++ Chunk.fromArray(payload) ++ checksum
  }

  def isAggregatedRecord(data: Chunk[Byte]): Boolean =
    data.slice(0, magicBytes.length).toArray sameElements magicBytes

  def decodeAggregatedRecord(dataChunk: Chunk[Byte]): Try[AggregatedRecord] =
    if (!isAggregatedRecord(dataChunk))
      Failure(new IllegalArgumentException("Data is not an aggregated record"))
    else {
      val payload  = dataChunk.slice(magicBytes.length, dataChunk.size - 16)
      val checksum = dataChunk.slice(dataChunk.size - 16, dataChunk.size)

      val calculatedChecksum = Chunk.fromArray(Md5Utils.computeMD5Hash(payload.toArray))

      if (calculatedChecksum != checksum)
        Failure(
          new IllegalArgumentException(
            s"Aggregated record checksum unexpected: ${checksum.mkString("-")} vs ${calculatedChecksum.mkString("-")}"
          )
        )
      else
        Try(Messages.AggregatedRecord.parseFrom(payload.toArray))
    }

}
