package nl.vroste.zio.kinesis.client.examples

import nl.vroste.zio.kinesis.client.serde.Serde
import zio.{ UIO, ZIO }

import scala.util.{ Failure, Success, Try }

case class TestMsg(id: String)

object TrySerde {

  val jsonSerde: Serde[Any, Try[TestMsg]] =
    Serde.asciiString.inmapM[Any, Try[TestMsg]](s => toModelM(s))(f => modelAsStringM(f))

  def toModelM(
    s: String
  ): ZIO[Any, Nothing, Try[TestMsg]] =
    for {
      a <- UIO(
             if (s == """{"id": "1"}""")
               Success(TestMsg("1"))
             else {
               println(s"XXXXXXXXXXXXXXXXXX $s")
               Failure(new Exception("Boom!"))
             }
           )
    } yield a

  def modelAsStringM(a: Try[TestMsg]): ZIO[Any, Nothing, String] =
    ZIO.effectTotal(a match {
      case Success(value) => """{"id": "1"}"""
      case Failure(_)     => "" // TODO - .die ?
    })

  /*
  def modelAsStringM(a: Try[TestMsg]): ZIO[Any, Throwable, String] =
    ZIO.fromTry(a match {
      case Success(value) if value == "1" => Success("""{"id": "1"}""")
      case Failure(_)                     => Failure(new Exception("Boom Boom!"))
    })
   */
}
