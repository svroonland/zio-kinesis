# ZIO Kinesis

ZIO Kinesis is a ZIO-based wrapper around the AWS Kinesis SDK. All operations are non-blocking. It provides a streaming interface to Kinesis streams.


## Status

Currently a work in progress. The API will likely change.

Ideally this library can offer equivalent functionality of the AWS Kinesis Client Library (checkpointing and rebalancing).


## Usage example

Process all shards of a stream from the beginning, using an existing registered consumer.

```scala
import nl.vroste.zio.kinesis.client.Client
import nl.vroste.zio.kinesis.client.serde.Serde
import software.amazon.awssdk.services.kinesis.model.{ ListShardsRequest, ShardIteratorType, StartingPosition }
import zio.Task

val streamName  = "my_stream"
val consumerARN = "arn:aws:etc"

Client.create.use { client =>
val stream = client
  .listShards(ListShardsRequest.builder().streamName("zio-test").build())
  .map { shard =>
    client.subscribeToShard(
      consumerARN,
      shard.shardId(),
      StartingPosition.builder().`type`(ShardIteratorType.TRIM_HORIZON).build(),
      Serde.asciiString
    )
  }
  .flatMapPar(Int.MaxValue) { shardStream =>
    shardStream.mapM { record =>
      // Do something with the record here
      // println(record.data)
      // and finally checkpoint the sequence number
      // customCheckpointer.checkpoint(record.shardID, record.sequenceNumber)
      Task.unit
    }
  }

stream.runDrain
}
```

Refer to the [AWS Kinesis Streams API Reference](https://docs.aws.amazon.com/kinesis/latest/APIReference/Welcome.html) for more information.

## Credits

The Serde construct in this library is inspired by [zio-kafka](https://github.com/zio/zio-kafka)


