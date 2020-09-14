package nl.vroste.zio.kinesis.client.examples.app.adhoc

import zio.{ UIO, ZLayer }
import zio.console.Console

object KinesisLoggerTest {

  val test = ZLayer.fromService[Console.Service, KinesisLogger.Service] { console =>
    new KinesisLogger.Service {
      override def debug(m: => String): UIO[Unit] = console.putStrLn(m)

      override def info(m: => String): UIO[Unit] = console.putStrLn(m)

      override def error(m: => String): UIO[Unit] = console.putStrLn(m)
    }
  }

}
