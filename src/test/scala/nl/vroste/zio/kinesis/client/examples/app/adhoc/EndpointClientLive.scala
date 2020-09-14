package nl.vroste.zio.kinesis.client.examples.app.adhoc

import java.net.URI

import io.circe.generic.auto._
import sttp.client._
import sttp.client.asynchttpclient.WebSocketHandler
import sttp.client.asynchttpclient.zio.SttpClient
import sttp.client.circe._
import sttp.model.Uri
import zio.{ Task, ZIO, ZLayer }

object EndpointClientLive {

  val layer: ZLayer[Config with SttpClient, Nothing, EndpointClient] =
    ZLayer
      .fromServices[Config.Service, SttpBackend[Task, Nothing, WebSocketHandler], EndpointClient.Service] {
        (config, sttpClient) =>
          new EndpointClient.Service {
            override def put(testMsg: TestMsg): ZIO[Any, Throwable, Unit] =
              for {
                uri        <- putUri
                httpRequest = basicRequest.put(Uri(uri)).body(testMsg)
                _          <- makePutRequest(httpRequest)
              } yield ()

            override def getAll: ZIO[Any, Throwable, Seq[TestMsg]] =
              for {
                uri        <- getUri
                httpRequest = basicRequest.get(Uri(uri))
                resp       <- makeGetRequest(httpRequest)
              } yield resp

            private val getUri = url("get")
            private val putUri = url("put")

            private def url(op: String): ZIO[Any, Throwable, URI] =
              for {
                c <- config.data
              } yield URI.create(s"http://${c.server.host}:${c.server.port}/$op")

            private def makePutRequest(
              httpRequest: Request[Either[String, String], Nothing]
            ) =
              (for {
                response <- sttpClient.send(httpRequest)
                _        <- ZIO.fromEither(response.body)
              } yield ()).mapError {
                case e: String => new RuntimeException(s"${httpRequest.method} request to invoice-service failed: $e")
              }

            private def makeGetRequest(
              httpRequest: Request[Either[String, String], Nothing]
            ) =
              (for {
                response     <- sttpClient.send(httpRequest.response(asJson[Seq[TestMsg]]))
                responseBody <- ZIO.fromEither(response.body)
              } yield responseBody)
                .mapError(t => new RuntimeException(s"${httpRequest.method} request to invoice-service failed: $t"))
          }
      }

}
