package nl.vroste.zio.kinesis.client.examples.app.adhoc

import zio.{ Tagged, ZIO, ZLayer }

object ProcessRecordAdhoc {

  def layer(implicit
    tag: Tagged[ProcessRecord.Service[model.TestMsg]]
  ): ZLayer[EndpointClient, Nothing, ProcessRecord[model.TestMsg]] =
    ZLayer.fromService[EndpointClient.Service, ProcessRecord.Service[model.TestMsg]] { endpointClient =>
      new ProcessRecord.Service[model.TestMsg] {
        override def process(rec: TestMsg): ZIO[Any, Throwable, Unit] = endpointClient.put(rec)
      }
    }

}
