package nl.vroste.zio.kinesis.client.examples.app.adhoc.apps

import nl.vroste.zio.kinesis.client.Client
import zio.console.Console
import zio.{ App, ZIO }

object ProducerApp extends App {

  private val program =
    for {
      c         <- config
      streamName = c.kinesis.streamName
      _         <- (Client.create <* createStream(streamName, c.kinesis.shardCount) <* mgdDynamoDbTableCleanUp(
               c.kinesis.appName
             )).use { client =>
             for {
               _ <- putStrLn("Stream created... run Consumer, then hit Enter to continue")
               _ <- getStrLn
               _ <- putRecordsEmitter(streamName, c.kinesis.batchSize, c.kinesis.totalRecords, client).runDrain
               _ <- putStrLn(
                      "Enter any key to continue. Stream and dynamoDB shard lease table will be deleted"
                    )
               _ <- getStrLn
             } yield ()
           }
    } yield ()

  override def run(args: List[String]): ZIO[zio.ZEnv, Nothing, Int] = {
    val kinesisLoggerLayer = Console.live >>> KinesisLoggerTest.test
    val layer              = ConfigDefault.layer ++ kinesisLoggerLayer

    program
      .catchAll(t => info(s"Error: $t").as(1))
      .provideCustomLayer(layer)
      .as(0)
  }
}
