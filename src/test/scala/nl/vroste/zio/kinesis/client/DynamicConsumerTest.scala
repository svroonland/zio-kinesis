package nl.vroste.zio.kinesis.client

import nl.vroste.zio.kinesis.client.serde.Serde
import zio.ZIO
import zio.test._

object DynamicConsumerTest
    extends DefaultRunnableSpec(
      suite("DynamicConsumer")(
        testM("basic") {
          println("Runnign test")
          val stream = DynamicConsumer
            .shardedStream("steven-test", "zio-test", Serde.asciiString)

          stream
            .flatMapPar(Int.MaxValue)(_._2.flattenChunks)
            .take(2)
            .foreach(r => ZIO(println(s"Got record ${r}")) *> r.checkpoint) *> ZIO(assertCompletes)
        }
      )
    )
