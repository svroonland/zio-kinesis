package nl.vroste.zio.kinesis.client.examples.app.adhoc

import zio.{ Layer, ZIO, ZLayer }

object ConfigDefault {

  val layer: Layer[Nothing, Config] = ZLayer.succeed {
    new Config.Service {
      override def data: ZIO[Any, Throwable, ConfigData] = ZIO.succeed(ConfigData())
    }
  }
}
