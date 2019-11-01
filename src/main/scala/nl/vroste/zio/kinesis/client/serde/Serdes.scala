package nl.vroste.zio.kinesis.client.serde

import java.nio.charset.StandardCharsets

import zio.ZIO

private[serde] trait Serdes {
  val byteArray: Serde[Any, Array[Byte]] = Serde(ZIO.succeed(_))(ZIO.succeed(_))
  val asciiString: Serde[Any, String] =
    byteArray.inmap(new String(_, StandardCharsets.US_ASCII))(_.getBytes(StandardCharsets.US_ASCII))
}
