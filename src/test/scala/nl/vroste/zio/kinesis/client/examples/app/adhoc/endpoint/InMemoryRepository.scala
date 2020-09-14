package nl.vroste.zio.kinesis.client.examples.app.adhoc.endpoint

import zio.{ Ref, ZIO, ZLayer }

object InMemoryRepository {

  def inMemory(
    refDb: Ref[Vector[TestMsg]],
    refFailOnIdList: Ref[Vector[Int]]
  ) =
    ZLayer.fromService[KinesisLogger.Service, Repository.Service] { logger =>
      new Repository.Service {
        override def put(testMsg: TestMsg): ZIO[Any, Throwable, Unit] =
          for {
            _ <- logger.info(s"putting record $testMsg")
            _ <- refDb.update(vector => vector :+ testMsg)
          } yield ()

        override def getAll: ZIO[Any, Throwable, Seq[TestMsg]] = refDb.get

        override def putFailOnIds(ids: Seq[Int]): ZIO[Any, Throwable, Unit] =
          refFailOnIdList.update(vector => vector ++ ids).unit

        override def deleteFailOnIds(): ZIO[Any, Throwable, Unit] =
          refFailOnIdList.update(_ => Vector.empty).unit

        override def getFailOnIds: ZIO[Any, Throwable, Seq[Int]] = refFailOnIdList.get

      }
    }

}
