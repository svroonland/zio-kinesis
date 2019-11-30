package nl.vroste.zio.kinesis.client.serde

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import zio.ZIO

private[serde] trait Serdes {
  val byteBuffer: Serde[Any, ByteBuffer] = Serde(ZIO.succeed(_))(ZIO.succeed(_))
  val asciiString: Serde[Any, String] =
    byteBuffer.inmap(StandardCharsets.US_ASCII.decode(_).toString)(StandardCharsets.US_ASCII.encode)
}
