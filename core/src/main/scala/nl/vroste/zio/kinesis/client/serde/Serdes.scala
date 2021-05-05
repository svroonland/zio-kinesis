package nl.vroste.zio.kinesis.client.serde

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import zio.{ Chunk, ZIO }

private[serde] trait Serdes {
  val byteBuffer: Serde[Any, ByteBuffer] = Serde(ZIO.succeed(_))(ZIO.succeed(_))
  val bytes: Serde[Any, Chunk[Byte]]     =
    Serde(byteBuffer => ZIO.succeed(Chunk.fromByteBuffer(byteBuffer)))(chunk =>
      ZIO.succeed(ByteBuffer.wrap(chunk.toArray))
    )
  val asciiString: Serde[Any, String]    =
    byteBuffer.inmap(StandardCharsets.US_ASCII.decode(_).toString)(StandardCharsets.US_ASCII.encode)
}
