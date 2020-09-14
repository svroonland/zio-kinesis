package nl.vroste.zio.kinesis.client.examples.app.adhoc.apps

import sttp.client.asynchttpclient.zio.AsyncHttpClientZioBackend
import zio._
import zio.logging.slf4j.Slf4jLogger

object ConsumerApp extends App {

  private val program: ZIO[ZioStreamClient[TestMsg] with Config with KinesisLogger, Throwable, Unit] = for {
    _ <- info("Consumer .... about to start consuming")
    c <- config
    _ <- ZIO.accessM[ZioStreamClient[model.TestMsg]](
           _.get
             .run(
               c.kinesis.streamName,
               c.kinesis.appName,
               model.TestMsgJsonSerde.jsonSerde
             )
             .provideLayer(zio.blocking.Blocking.live)
         )

  } yield ()

  override def run(args: List[String]): ZIO[zio.ZEnv, Nothing, Int] = {

    val zioLogger            = Slf4jLogger.make((_, message) => message)
    val sttpLayer            = AsyncHttpClientZioBackend.layer()
    val endpointLayer        = (Config.live ++ sttpLayer) >>> EndpointClient.live
    val kinesisLoggerLayer   = zioLogger >>> KinesisLoggerAdaptor.adapter
    val kinesisConsumerLayer = KinesisConsumerLive.layer[model.TestMsg]
    val processRecordLayer   = endpointLayer >>> ProcessRecordAdhoc.layer
    val zioStreamClientLayer =
      (kinesisLoggerLayer ++ kinesisConsumerLayer ++ processRecordLayer) >>> ZioStreamClientLive
        .layer[model.TestMsg]()
    val layer                = zioStreamClientLayer ++ ConfigDefault.layer ++ kinesisLoggerLayer

    program.provideCustomLayer(layer).ignore.as(0)
  }
}
