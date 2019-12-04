# ZIO Kinesis

ZIO Kinesis is a ZIO-based wrapper around the AWS Kinesis SDK. All operations are non-blocking. It provides a streaming interface to Kinesis streams.


## Features

The library consists of 3 major components:

* `Client` and `AdminClient`: ZIO wrappers around the low level AWS Kinesis SDK methods. Methods offer a ZIO-native interface with ZStream where applicable, taking care of paginated request and AWS rate limits.
* `DynamicConsumer`: a ZStream-based interface to the Kinesis Client Library; an auto-rebalancing and checkpointing consumer.
* `Producer`: used to produce efficiently and reliably to Kinesis while respecting Kinesis limits. Features batching and failure handling.


## Client and AdminClient 

Add to your build.sbt:

```scala
libraryDependencies += "nl.vroste" %% "zio-kinesis" % "0.3.0"
```

## DynamicConsumer
`DynamicConsumer` offers a `ZStream`-based interface to the Kinesis Client Library (KCL). KCL supports shard offset checkpoint storage in DynamoDB and automatic rebalancing of shard consumers between multiple workers within an application group. 

This is modeled as a stream of streams, where the inner streams represent the individual shards. They can complete when the shard is assigned to another worker. The outer stream can emit new elements as shards are assigned to this worker. The inner streams can be processed in parallel as you desire.

`DynamicConsumer` will handle deserialization of the data bytes as part of the stream via the `Deserializer` (or `Serde`) you pass it. In the example below a deserializer for ASCII strings is used. It's easy to define custom (de)serializers for, for example, JSON data using a JSON library of your choice.

Usage example:

```scala
import zio._
import nl.vroste.zio.kinesis.client.DynamicConsumer
import nl.vroste.zio.kinesis.client.serde.Serde

val streamName  = "my_stream"
val applicationName ="my_awesome_zio_application"

DynamicConsumer
  .shardedStream(
    streamName,
    applicationName = applicationName,
    deserializer = Serde.asciiString
  )
  .flatMapPar(Int.MaxValue) { case (shardId: String, shardStream) => 
    shardStream
      .tap { r: DynamicConsumer.Record[String] =>
          ZIO(println(s"Got record ${r} on shard ${shardId}")) *> r.checkpoint
        }
      .flattenChunks
  }
  .runDrain
```

DynamicConsumer is built on `ZManaged` and therefore resource-safe: after stream completion all resources acquired will be shutdown.

## Producer
The low-level `Client` offers a `putRecords` method to put records on Kinesis. Although simple to use for a small number of records, there are many catches when it comes to efficiently and reliably producing a high volume of records. 

`Producer` helps you achieve high throughput by batching records and respecting the Kinesis request (rate) limits. Records that cannot be produced due to temporary errors, like shard rate limits, will be retried.

All of this of course with the robust failure handling you can expect from a ZIO-based library.

Usage example:


```scala
import zio._
import nl.vroste.zio.kinesis.client._
import serde._
import Client.ProducerRecord

val streamName  = "my_stream"
val applicationName ="my_awesome_zio_application"

(for {
    client <- Client.create
    producer <- Producer
                 .make(streamName, client, Serde.asciiString)
} yield producer).use { producer =>
    val records = (1 to 100).map(j => ProducerRecord(s"key${j}", s"message${j}"))
    producer
      .produceChunk(Chunk.fromIterable(records)) *> 
        ZIO(println(s"All records in the chunk were produced"))
}
```

### Consuming a stream (low level)
This example shows how the low-level `Client` can be used for more control over the consuming process. 

Process all shards of a stream from the beginning, using an existing registered consumer. You will have to track current shard positions yourself using some external storage mechanism.

```scala
import nl.vroste.zio.kinesis.client.Client
import nl.vroste.zio.kinesis.client.Client.ShardIteratorType
import nl.vroste.zio.kinesis.client.serde.Serde
import zio.Task

val streamName  = "my_stream"
val consumerARN = "arn:aws:etc"

Client.create.use { client =>
    val stream = client
      .listShards("zio-test")
      .map { shard =>
        client.subscribeToShard(
          consumerARN,
          shard.shardId(),
          ShardIteratorType.TrimHorizon,
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

### Admin operations
The more administrative operations like creating and deleting streams are available in the `AdminClient`.

Refer to the [AWS Kinesis Streams API Reference](https://docs.aws.amazon.com/kinesis/latest/APIReference/Welcome.html) for more information.

### Configuration
By default `Client`, `AdminClient`, `DynamicConsumer` and `Producer` will load AWS credentials and regions via the [Default Credential/Region Provider](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html). Using the client builders, many parameters can be customized. Refer to the AWS documentation for more information.

### More usage examples

Refer to the [unit tests](src/test/scala/nl/vroste/zio/kinesis/client).

## Credits

The Serde construct in this library is inspired by [zio-kafka](https://github.com/zio/zio-kafka), the producer by [this AWS blog post](https://aws.amazon.com/blogs/big-data/implementing-efficient-and-reliable-producers-with-the-amazon-kinesis-producer-library/)


