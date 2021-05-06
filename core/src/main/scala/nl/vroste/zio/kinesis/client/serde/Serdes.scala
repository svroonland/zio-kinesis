package nl.vroste.zio.kinesis.client.serde

import java.nio.charset.StandardCharsets
import zio.{ Chunk, ZIO }

import java.nio.ByteBuffer

private[serde] trait Serdes {
  val byteBuffer: Serde[Any, ByteBuffer] = Serde(chunk => ZIO.succeed(ByteBuffer.wrap(chunk.toArray)))(byteBuffer =>
    ZIO.succeed(Chunk.fromByteBuffer(byteBuffer))
  )

  val bytes: Serde[Any, Chunk[Byte]] =
    Serde(ZIO.succeed(_))(ZIO.succeed(_))

  val asciiString: Serde[Any, String] =
    bytes.inmap(chunk => new String(chunk.toArray, StandardCharsets.US_ASCII))(string =>
      Chunk.fromArray(string.getBytes)
    )
}
