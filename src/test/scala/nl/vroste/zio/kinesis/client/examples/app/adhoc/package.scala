package nl.vroste.zio.kinesis.client.examples.app

import java.io.IOException

import zio.ZIO
import zio.console.Console

package object adhoc {

  def info(msg: => String): ZIO[KinesisLogger, Nothing, Unit]  = ZIO.accessM[KinesisLogger](_.get.info(msg))
  def error(msg: => String): ZIO[KinesisLogger, Nothing, Unit] = ZIO.accessM[KinesisLogger](_.get.error(msg))

  def config: ZIO[Config, Throwable, ConfigData] = ZIO.accessM[Config](_.get.data)

  def putStrLn(s: String): ZIO[Console, Nothing, Unit] = ZIO.accessM[Console](_.get.putStrLn(s))
  def getStrLn: ZIO[Console, IOException, String]      = ZIO.accessM[Console](_.get.getStrLn)
}
