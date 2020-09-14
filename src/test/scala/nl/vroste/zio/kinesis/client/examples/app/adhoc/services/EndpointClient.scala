package nl.vroste.zio.kinesis.client.examples.app.adhoc.services

import zio.ZIO

object EndpointClient {

  trait Service {
    def put(testMsg: TestMsg): ZIO[Any, Throwable, Unit]
    def getAll: ZIO[Any, Throwable, Seq[TestMsg]]
  }

  val live = EndpointClientLive.layer

}
