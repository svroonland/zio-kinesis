package nl.vroste.zio.kinesis.client

import nl.vroste.zio.kinesis.client.serde.Serde
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import zio.ZIO
import zio.test._

object DynamicConsumerTest
    extends DefaultRunnableSpec(
      suite("DynamicConsumer")(
        testM("basic") {
          println("Runnign test")
          val stream = DynamicConsumer
            .stream("steven-test", "zio-test", KinesisAsyncClient.builder(), Region.EU_WEST_1, Serde.asciiString)

          stream
            .flatMapPar(Int.MaxValue)(_._2.flattenChunks)
            .take(2)
            .foreach(r => ZIO(println(s"Got record ${r}")) *> r.checkpoint) *> ZIO(assertCompletes)
        }
      )
    )
