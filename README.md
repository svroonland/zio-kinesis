# ZIO Kinesis

ZIO Kinesis is a ZIO-based wrapper around the AWS Kinesis SDK. All operations are non-blocking. It provides a streaming interface to Kinesis streams.


## Status

Currently a work in progress. The API will likely change.

Ideally this library can offer equivalent functionality of the AWS Kinesis Client Library (checkpointing and rebalancing).


## Usage example

Process all shards of a stream from the beginning, using an existing registered consumer.

```scala
import nl.vroste.zio.kinesis.client.serde.Serde

import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.{ ShardIteratorType, StartingPosition }
import zio.Task
import nl.vroste.zio.kinesis.client._

val streamName = "my_stream"
val consumerARN = "arn:aws:etc"

val client = Client.create

client.use { c => 
  val stream = c.consumeStream(
      consumerARN = consumerARN,
      streamName = streamName,
      shardStartingPositions = _ =>
        Task.succeed(
          StartingPosition.builder().`type`(ShardIteratorType.TRIM_HORIZON).build()
        ),
      serde = Serde.asciiString
    )
    .flatMapPar(Int.MaxValue) { shardStream => shardStream }
  stream.tap(record => Task(println(record))).runDrain
}
```

Refer to the [AWS Kinesis Streams API Reference](https://docs.aws.amazon.com/kinesis/latest/APIReference/Welcome.html) for more information.

## Credits

The Serde construct in this library is inspired by [zio-kafka](https://github.com/zio/zio-kafka)


