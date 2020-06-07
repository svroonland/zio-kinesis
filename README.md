[![Bintray](https://img.shields.io/bintray/v/vroste/maven/zio-kinesis?label=latest)](https://bintray.com/vroste/maven/zio-kinesis/_latestVersion)

# ZIO Kinesis

ZIO Kinesis is a ZIO-based wrapper around the AWS Kinesis SDK. All operations are non-blocking. It provides a streaming interface to Kinesis streams.

The project is in beta stage. Although already being used in production by a small number of organisations, expect some issues to pop up.
More beta customers are welcome.

* [Features](#features)
* [Installation](#installation)
* [DynamicConsumer](#dynamicconsumer)
  + [Checkpointing](#checkpointing)
  + [Clean Shutdown](#clean-shutdown)
  + [Configuration](#configuration)
* [Producer](#producer)
* [Consuming a stream (low level)](#consuming-a-stream--low-level-)
* [Admin client](#admin-client)
* [Running tests and usage examples](#running-tests-and-usage-examples)
* [Credits](#credits)

## Features

The library consists of 3 major components:

* `Client` and `AdminClient`: ZIO wrappers around the low level AWS Kinesis SDK methods. Methods offer a ZIO-native interface with ZStream where applicable, taking care of paginated request and AWS rate limits.
* `DynamicConsumer`: a ZStream-based interface to the Kinesis Client Library; an auto-rebalancing and checkpointing consumer.
* `Producer`: used to produce efficiently and reliably to Kinesis while respecting Kinesis limits. Features batching and failure handling.


## Installation

Add to your build.sbt:

```scala
resolvers += Resolver.jcenterRepo
libraryDependencies += "nl.vroste" %% "zio-kinesis" % "<version>"
```

## DynamicConsumer
`DynamicConsumer` offers a `ZStream`-based interface to Kinesis Streams, backed by AWS's [Kinesis Client Library (KCL)](https://docs.aws.amazon.com/streams/latest/dev/shared-throughput-kcl-consumers.html). It supports supports shard sequence number checkpoint storage in DynamoDB and automatic rebalancing of shard consumers between multiple workers within an application group. 

This is modeled as a stream of streams, where the inner streams represent the individual shards. The inner streams can complete when the shard is assigned to another worker or the shard is ended. The outer stream can emit new elements as shards are assigned to this worker or the stream is resharded. The inner streams can be processed in parallel.

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
  }
  .runDrain
```

DynamicConsumer is built on `ZManaged` and therefore resource-safe: after stream completion all resources acquired will be shutdown.

### Checkpointing

You need to manually checkpoints records that your application has processed so far. Kinesis works with sequence numbers instead of something like ACKs; checkpointing for sequence number X means 'all records up to and including X'. Therefore you don't have to checkpoint each individual record, periodic checkpointing is sufficient.

In fact, it is [recommended](https://github.com/awslabs/amazon-kinesis-client/blob/master/amazon-kinesis-client/src/main/java/software/amazon/kinesis/processor/RecordProcessorCheckpointer.java#L35)
not to checkpoint too frequently. It depends on your application and stream volume what is a good checkpoint frequency (in terms of number of records and/or interval). ZStream's `aggregateAsyncWithin` is useful for such a checkpointing scheme.

`zio-kinesis` has some mechanisms to improve checkpointing safety in the case of interruption or failures:

* To guarantee that the last processed records is checkpointed when the stream shuts down, because of failure or interruption for example, checkpoints for every record should be staged by calling `checkpointer.stage(record)`. A periodic call to `checkpointer.checkpoint` will 'flush' the last staged checkpoint.
* To ensure that processing of a record is always followed by a checkpoint stage, even in the face of fiber interruption, use the utility method `Checkpointer.stageOnSuccess(processingEffect)(r)`. 

The example below shows how to combine this and checkpoint every max every 500 records or 1 second, whichever comes sooner:

```scala
DynamicConsumer
  .shardedStream(
    streamName,
    applicationName = applicationName,
    deserializer = Serde.byteBuffer
  )
  .flatMapPar(maxParallel) {
    case (shardId: String, shardStream: ZStream[Any, Throwable, DynamicConsumer.Record[ByteBuffer]], checkpointer: Checkpointer) =>
      shardStream
        .tap { record => checkpointer.stageOnSuccess(processMyRecord(shardId, record))(record) }
        .as(())
        .aggregateAsyncWithin(ZTransducer.collectAllN(500), Schedule.fixed(1.second))
        .mapConcat(_.toList)
        .tap(_ => checkpointer.checkpoint)
  }
  .runDrain 
```

Checkpointing may fail with a `ShutdownException` when another worker has stolen the lease for a shard. Your application should handle this, otherwise your stream will fail with this exception. Note that the shard stream may still emit some buffered records in this situation, before it is completed. 
 
### Clean Shutdown
It is nice to ensure that every record that is (side-effectfully) processed is checkpointed before the stream is shutdown. The method of shutdown is therefore important.

Simply interrupting the fiber that is running the stream will terminate the stream, but will not guarantee that the last processed records have been checkpointed. Instead use the `requestShutdown` parameter of `DynamicCustomer.shardedStream` to pass a ZIO (or a Promise followed by `.await`) that completes when the stream should be shutdown. 

Use `withGracefulShutdownOnInterrupt` from the `nl.vroste.zio.kinesis.client` package to help with this. See `src/test/scala/nl/vroste/zio/kinesis/client/ExampleApp.scala` for an example.

It is also important that you perform checkpointing before merging the shard streams (using eg `flatMapPar`) to guarantee that the KCL has not taken away the lease for that shard when checkpointing. The example above does this correctly.

Note that `plainStream` does not support this scheme, since it checkpoints after merging the shard streams. At shutdown, there may no longer be a valid lease for each of the shards. 

### Configuration
By default `Client`, `AdminClient`, `DynamicConsumer` and `Producer` will load AWS credentials and regions via the [Default Credential/Region Provider](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html). Using the client builders, many parameters can be customized. Refer to the AWS documentation for more information.

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
    requestShutdown = UIO.never,
    initialPosition = initialPosition,
    isEnhancedFanout = true,
    leaseTableName = None,
    workerIdentifier = "machine-001",
    maxShardBufferSize = 1024 
  )
```

The KCL underlying `DynamicConsumer.shardedStream` by default starts with the oldest message on each shard (`TRIM_HORIZON`). The initial position is only used during initial lease creation. When an application restarts, it will resume from the previous checkpoint,
  and so will continue from where it left off in the Kinesis stream.

[Enhanced Fan Out capability](https://docs.aws.amazon.com/streams/latest/dev/enhanced-consumers.html) is set by the
 `isEnhancedFanOut` flag, which defaults to `true`.   

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

## Consuming a stream (low level)
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

## Admin client
The more administrative operations like creating and deleting streams are available in the `AdminClient`.

Refer to the [AWS Kinesis Streams API Reference](https://docs.aws.amazon.com/kinesis/latest/APIReference/Welcome.html) for more information.

## Running tests and usage examples 

[Note the tests are also good usage examples](src/test/scala/nl/vroste/zio/kinesis/client)

The tests run against a [`localstack`](https://github.com/localstack/localstack) docker image to access 
`kinesis`, `dynamoDb` and `cloudwatch` endpoints locally. In order to run the tests you need to have `docker` and `docker-compose` 
installed on your machine. Then on your machine open a terminal window and navigate to the root of this project and type: 

    > docker-compose -f docker/docker-compose.yml up -d
    
To run the tests, enter the following in the terminal:

    > sbt test   
    
Don't forget to shut down the docker container after you have finished. In the terminal type:     

    > docker-compose -f docker/docker-compose.yml down

## Credits

The Serde construct in this library is inspired by [zio-kafka](https://github.com/zio/zio-kafka), the producer by [this AWS blog post](https://aws.amazon.com/blogs/big-data/implementing-efficient-and-reliable-producers-with-the-amazon-kinesis-producer-library/)
