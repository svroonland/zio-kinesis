package nl.vroste.zio.kinesis.client.examples

import nl.vroste.zio.kinesis.client.serde.Serde
import zio.{ UIO, ZIO }

import scala.util.{ Failure, Success, Try }

case class TestMsg(id: String)
object TestMsg {
  def parse(msg: String): Try[TestMsg] =
    if (msg == """{"id": "1"}""") // hand rolled noddy Json parser - only parses this Json element
      Success(TestMsg("1"))
    else {
      println(s"XXXXXXXXXXXXXXXXXX $msg")
      Failure(new Exception("Boom!"))
    }
}

object TrySerde {

  val jsonSerde: Serde[Any, Try[TestMsg]] =
    Serde.asciiString.inmapM[Any, Try[TestMsg]](s => toModelM(s))(f => modelAsStringM(f))

  def toModelM(
    s: String
  ): ZIO[Any, Nothing, Try[TestMsg]] =
    UIO(TestMsg.parse(s))

  def modelAsStringM(a: Try[TestMsg]): ZIO[Any, Throwable, String] =
    ZIO.fromTry(a match {
      case Success(value) if value == TestMsg("1") => Success("""{"id": "1"}""")
      case Failure(_)                              => Failure(new Exception("Boom Boom!"))
    })
}
