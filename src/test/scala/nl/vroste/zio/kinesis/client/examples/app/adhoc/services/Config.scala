package nl.vroste.zio.kinesis.client.examples.app.adhoc.services

import zio.{ Layer, ZIO }

object Config {

  trait Service {
    def data: ZIO[Any, Throwable, ConfigData]
  }

  val live: Layer[Nothing, Config] = ConfigDefault.layer
}
