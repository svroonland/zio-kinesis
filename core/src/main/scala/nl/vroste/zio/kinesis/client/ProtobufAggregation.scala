package nl.vroste.zio.kinesis.client
import com.google.protobuf.UnsafeByteOperations
import nl.vroste.zio.kinesis.client.zionative.protobuf.Messages
import nl.vroste.zio.kinesis.client.zionative.protobuf.Messages.AggregatedRecord
import software.amazon.awssdk.utils.Md5Utils
import zio.Chunk

import java.security.MessageDigest
import scala.util.{ Failure, Try }

object ProtobufAggregation {
  // From https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md
  val magicBytes: Array[Byte] = List(0xf3, 0x89, 0x9a, 0xc2).map(_.toByte).toArray
  val checksumSize            = 16

  @inline
  def putRecordsRequestEntryToRecord(
    data: Chunk[Byte],
    explicitHashKey: Option[String],
    tableIndex: Int
  ): Messages.Record = {
    val b = Messages.Record
      .newBuilder()
      .setData(UnsafeByteOperations.unsafeWrap(data.toArray)) // Safe because chunks are immutable
      .setPartitionKeyIndex(tableIndex.toLong)

    explicitHashKey
      .fold(b)(_ => b.setExplicitHashKeyIndex(tableIndex.toLong))
      .build()
  }

  def encodedSize(ar: AggregatedRecord): Int =
    magicBytes.length + ar.getSerializedSize + checksumSize

  def encodeAggregatedRecord(digest: MessageDigest, ar: AggregatedRecord): Chunk[Byte] = {
    val payload  = ar.toByteArray
    val checksum = Chunk.fromArray(digest.digest(payload))
    Chunk.fromArray(magicBytes) ++ Chunk.fromArray(payload) ++ checksum
  }

  def isAggregatedRecord(data: Chunk[Byte]): Boolean =
    data.slice(0, magicBytes.length).toArray sameElements magicBytes

  def decodeAggregatedRecord(dataChunk: Chunk[Byte]): Try[AggregatedRecord] =
    if (!isAggregatedRecord(dataChunk))
      Failure(new IllegalArgumentException("Data is not an aggregated record"))
    else {
      val payload  = dataChunk.slice(magicBytes.length, dataChunk.size - checksumSize)
      val checksum = dataChunk.slice(dataChunk.size - checksumSize, dataChunk.size)

      // TODO do not instantiate MD5 digest every time
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
