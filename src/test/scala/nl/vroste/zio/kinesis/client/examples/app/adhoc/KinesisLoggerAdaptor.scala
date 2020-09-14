package nl.vroste.zio.kinesis.client.examples.app.adhoc

import zio.logging.{ LogLevel, Logging }
import zio.logging.Logging.Logging
import zio.{ UIO, ZLayer }

object KinesisLoggerAdaptor {

  val adapter: ZLayer[Logging, Nothing, KinesisLogger] =
    ZLayer.fromService[Logging.Service, KinesisLogger.Service] { logger =>
      new KinesisLogger.Service {
        override def debug(m: => String): UIO[Unit] = logger.logger.log(LogLevel.Debug)(m)

        override def info(m: => String): UIO[Unit] = logger.logger.log(LogLevel.Info)(m)

        override def error(m: => String): UIO[Unit] = logger.logger.log(LogLevel.Error)(m)
      }
    }

}
