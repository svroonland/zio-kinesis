package nl.vroste.zio.kinesis.client.examples.app.adhoc.endpoint

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import io.circe.Printer
import zio.{ Runtime, ZIO }

class Api(r: Runtime[Repository]) extends ZioSupport(r) {
  import FailFastCirceSupport._
  import io.circe.generic.auto._

  private implicit val printer: Printer = Printer.noSpaces.copy(dropNullValues = true)

  lazy val routes: Route = send ~ report ~ deleteFailOnIds ~ putFailOnIds ~ getFailOnIds

  private lazy val send: Route = put {
    path("put") {
      entity(as[TestMsg]) { msg =>
        val putM =
          for {
            failOnIds <- ZIO.accessM[Repository](_.get.getFailOnIds)
            resp      <- if (failOnIds.contains(msg.id))
                      ZIO.fail(new RuntimeException("BOOM!"))
                    else
                      ZIO.accessM[Repository](_.get.put(msg))
          } yield resp

        putM.fold(failureStatus => complete(failureStatus), _ => complete(s"PUT $msg"))
      }
    }
  }

  private lazy val report: Route = get {
    val recordsM: ZIO[Repository, Throwable, EndpointReport] = for {
      records   <- ZIO.accessM[Repository](_.get.getAll)
      failOnIds <- ZIO.accessM[Repository](_.get.getFailOnIds)
    } yield EndpointReport(records, failOnIds)

    path("get") {
      recordsM.fold(failureStatus => complete(failureStatus), records => complete(records))
    }
  }

  private lazy val putFailOnIds: Route = put {
    path("putFailOnIds") {
      entity(as[Seq[Int]]) { failOnIds =>
        val putM =
          for {
            _ <- ZIO.accessM[Repository](_.get.putFailOnIds(failOnIds))
          } yield ()

        putM.fold(failureStatus => complete(failureStatus), _ => complete(s"PUT $failOnIds"))
      }
    }
  }

  private lazy val deleteFailOnIds: Route = delete {
    val deleteM: ZIO[Repository, Throwable, Unit] = for {
      _ <- ZIO.accessM[Repository](_.get.deleteFailOnIds())
    } yield ()

    path("deleteFailOnIds") {
      deleteM.fold(failureStatus => complete(failureStatus), _ => complete(()))
    }
  }

  private lazy val getFailOnIds: Route = get {
    val recordsM: ZIO[Repository, Throwable, Seq[Int]] = for {
      records <- ZIO.accessM[Repository](_.get.getFailOnIds)
    } yield records

    path("getFailOnIds") {
      recordsM.fold(failureStatus => complete(failureStatus), records => complete(records))
    }
  }

}

case class EndpointReport(totalCount: Int, distinctCount: Int, failOnIds: Seq[Int])

object EndpointReport {

  def apply(records: Seq[TestMsg], failOnIds: Seq[Int]): EndpointReport =
    EndpointReport(records.size, records.distinct.size, failOnIds)
}
