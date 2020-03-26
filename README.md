[![Bintray](https://img.shields.io/bintray/v/vroste/maven/zio-kinesis?label=latest)](https://bintray.com/vroste/maven/zio-kinesis/_latestVersion)

# ZIO Kinesis

ZIO Kinesis is a ZIO-based wrapper around the AWS Kinesis SDK. All operations are non-blocking. It provides a streaming interface to Kinesis streams.

I wrote the library to learn more about Kinesis and ZIO, i'm not using it in a production project myself.
However, this open-source library is already being successfully used in production in one or two organisations.
More beta customers are welcome.

## Features

The library consists of 3 major components:

* `Client` and `AdminClient`: ZIO wrappers around the low level AWS Kinesis SDK methods. Methods offer a ZIO-native interface with ZStream where applicable, taking care of paginated request and AWS rate limits.
* `DynamicConsumer`: a ZStream-based interface to the Kinesis Client Library; an auto-rebalancing and checkpointing consumer.
* `Producer`: used to produce efficiently and reliably to Kinesis while respecting Kinesis limits. Features batching and failure handling.


## Client and AdminClient 

Add to your build.sbt:

```scala
libraryDependencies += "nl.vroste" %% "zio-kinesis" % "0.4.0"
```

Your SBT settings must specify the resolver for JCenter.  

```scala
  resolvers += Resolver.jcenterRepo
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


### Notes

- DynamicConsumer is built on `ZManaged` and therefore resource-safe: after stream completion all resources acquired will be shutdown.

- DynamicConsumer.shardedStream takes default value for initialPosition in the stream that the application should 
  start at = `TRIM_HORIZON`, which is from the oldest messages in Kinesis.
  However, from the KCL documentation, the initial position is only used during initial lease creation.
  When an application restarts, it will resume from the previous checkpoint,
  and so will continue from where it left off in the Kinesis stream.

#### Checkpoint coordination schemes

The handler for messages in the example above calls `r.checkpoint`. This checkpoints every message.
It is [recommended](https://github.com/awslabs/amazon-kinesis-client/blob/master/amazon-kinesis-client/src/main/java/software/amazon/kinesis/processor/RecordProcessorCheckpointer.java#L35)
to not checkpoint not too frequently.
Instead, a count-based or period-based checkpointing scheme should be used as shown as follows.

```scala
DynamicConsumer
  .shardedStream(
    streamName,
    applicationName = applicationName,
    deserializer = Serde.byteBuffer
  )
  .flatMapPar(maxParallel) {
    case (shardId: String, shardStream: ZStreamChunk[Any, Throwable, DynamicConsumer.Record[ByteBuffer]]) =>
      shardStream
        .zipWithIndex
        .tap {
          case (r: DynamicConsumer.Record[ByteBuffer], sequenceNumberForShard: Long) =>
            handler(shardId, r) *> (
              if (sequenceNumberForShard % checkpointDivisor == checkpointDivisor - 1) r.checkpoint
              else UIO.succeed(())
            )
        }
        .map(_._1) // remove sequence numbering
        .flattenChunks
  }
```

In this example, checkpointing is done once per batch of `checkpointDivisor` records. This batch counting is per-shard. 

#### Authentication with AWS

The following snippet shows the full range of parameters to `DynamicConsumer.shardedStream`, most of which relate
to authentication of the AWS resources.

```scala
val credentials = StaticCredentialsProvider.create(AwsBasicCredentials.create(awsKey, awsSecret))

val kinesisClientBuilder =
  KinesisAsyncClient
    .builder
    .credentialsProvider(credentials)
    .region(region)

val cloudWatchClientBuilder: CloudWatchAsyncClientBuilder =
  CloudWatchAsyncClient
    .builder
    .credentialsProvider(credentials)
    .region(region)

val dynamoDbClientBuilder =
  DynamoDbAsyncClient
    .builder
    .credentialsProvider(credentials)
    .region(region)

val initialPosition = InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON)

DynamicConsumer
  .shardedStream(
    streamName,
    applicationName = applicationName,
    deserializer = Serde.byteBuffer,
    kinesisClientBuilder = kinesisClientBuilder,
    cloudWatchClientBuilder = cloudWatchClientBuilder,
    dynamoDbClientBuilder = dynamoDbClientBuilder,
    initialPosition = initialPosition
  )

```

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
