package nl.vroste.zio.kinesis.client.examples.app.adhoc.services

import zio.ZIO

object Repository {

  trait Service {
    def put(testMsg: TestMsg): ZIO[Any, Throwable, Unit]
    def getAll: ZIO[Any, Throwable, Seq[TestMsg]]
    def putFailOnIds(ids: Seq[Int]): ZIO[Any, Throwable, Unit]
    def deleteFailOnIds(): ZIO[Any, Throwable, Unit]
    def getFailOnIds: ZIO[Any, Throwable, Seq[Int]]
  }
}
