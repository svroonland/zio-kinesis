package nl.vroste.zio.kinesis.client.examples.app.adhoc.model

import io.circe
import io.circe.parser.parse
import nl.vroste.zio.kinesis.client.serde.Serde
import zio.ZIO

object TestMsgJsonSerde {
  import io.circe.generic.auto._
  import io.circe.syntax._

  val jsonSerde: Serde[Any, TestMsg] =
    Serde.asciiString.inmapM[Any, TestMsg](s => toModelM(s))(f => modelAsStringM(f))

  private def toModelM(s: String): ZIO[Any, circe.Error, TestMsg] = {
    val either = for {
      json <- parse(s)
      a    <- json.as[TestMsg]
    } yield a
    ZIO.fromEither(either)
  }

  private def modelAsStringM(a: TestMsg): ZIO[Any, Nothing, String] =
    ZIO.effectTotal(a.asJson.toString)
}
