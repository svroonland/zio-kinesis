package nl.vroste.zio.kinesis.client

import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import zio.ZIO
import zio.stream.ZStream
import zio.test._
import zio.duration._
import zio.test.environment.Live

object DynamicConsumerTest
    extends DefaultRunnableSpec(
      suite("DynamicConsumer")(
//        testM("basic") {
//          println("Runnign test")
//          val stream = DynamicConsumer.stream("steven-test", "zio-test", KinesisAsyncClient.builder(), Region.EU_WEST_1)
//
//          stream
//            .flatMapPar(Int.MaxValue)(_._2.flattenChunks)
//            .take(2)
//            .foreach(r => ZIO(println(s"Got record ${r}"))) *> ZIO(assertCompletes)
//        },
        testM("reduced") {
          println("Runnign test")
          val stream =
            DynamicConsumer.streamTest("steven-test", "zio-test", KinesisAsyncClient.builder(), Region.EU_WEST_1)

          stream.use { _ =>
            Live.live(ZIO.sleep(20.seconds)) *>
              ZIO(println("Done")) *>
              ZIO(assertCompletes)
          }
        }
//        testM("stream finalize") {
//          val stream = ZStream.managed {
//            for {
//              _ <- ZIO.unit.toManaged(_ => ZIO(println("Stopping the unit here")).ignore)
//              _ <- ZIO.unit.toManaged(_ => ZIO(println("Stopping the unit 2 here")).ignore)
//            } yield ZStream.fromIterable(List(1, 2, 3, 4, 5, 6, 7, 8, 9))
//          }.flatMap { x =>
//            x
//          }.take(3)
//
//          stream.runDrain *> ZIO(assertCompletes)
//        }
      )
    )
