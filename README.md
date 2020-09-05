[![Bintray](https://img.shields.io/bintray/v/vroste/maven/zio-kinesis?label=latest)](https://bintray.com/vroste/maven/zio-kinesis/_latestVersion)

# ZIO Kinesis

ZIO Kinesis is a ZIO-based interface to Amazon Kinesis Data Streams for consuming and producing.

The project is in beta stage. Although already being used in production by a small number of organisations, expect some issues to pop up and some changes to the interface.
More beta users and feedback are of course welcome.

- [Features](#features)
- [Installation](#installation)
- [Consumer](#consumer)
  * [Basic usage](#basic-usage)
  * [Checkpointing](#checkpointing)
  * [Lease coordination](#lease-coordination)
  * [Resharding](#resharding)
  * [Consuming multiple Kinesis streams](#consuming-multiple-kinesis-streams)
  * [Customization](#customization)
  * [Diagnostic event & metrics](#diagnostic-event---metrics)
  * [KCL compatibility](#kcl-compatibility)
  * [Unsupported features](#unsupported-features)
- [Configuration](#configuration)
- [Producer](#producer)
  * [Metrics](#metrics)
- [DynamicConsumer](#dynamicconsumer)
  * [Basic usage using `consumeWith`](#basic-usage-using--consumewith-)
  * [Advanced usage](#advanced-usage)
- [Client and AdminClient](#client-and-adminclient)
- [Running tests and more usage examples](#running-tests-and-more-usage-examples)
- [Credits](#credits)

## Features

The library consists of:

* `Consumer`  
  A ZStream interface to Kinesis streams, including checkpointing , lease coordination and metrics. This is a ZIO-native client built on top of the AWS SDK, designed for compatibility with KCL clients. Both polling and enhanced fanout (HTTP2 streaming) are supported.
  
* `Producer`  
  Put records efficiently and reliably on Kinesis while respecting Kinesis throughput limits. Features batching and failure handling.

* `Client` and `AdminClient`  
  ZIO wrappers around the low level AWS SDK methods for Kinesis. Methods offer a ZIO-native interface with ZStream where applicable, taking care of paginated request and AWS rate limits. 
  
* `DynamicConsumer`  
  A ZStream-based wrapper around the Kinesis Client Library; an auto-rebalancing and checkpointing consumer.  
  _NOTE Although `DynamicConsumer` will be included in this library for some time to come, it will eventually be deprecated and removed in favour of the ZIO-native `Consumer`. Users are recommended to upgrade._


## Installation

Add to your build.sbt:

```scala
resolvers += Resolver.jcenterRepo
libraryDependencies += "nl.vroste" %% "zio-kinesis" % "<version>"
```

The latest version is built against ZIO 1.0.0-RC21-2.

## Consumer

`Consumer` offers a fully parallel streaming interface to Kinesis Data Streams. 

Features:
* Record streaming from multiple shards in parallel
* Multiple worker support: lease rebalancing / stealing / renewing (compatible with KCL)
* Polling fetching and enhanced fanout (HTTP2 streaming)
* Deserialization of records to any data type
* Checkpointing of records according to user-defined Schedules
* Automatic checkpointing at shard stream shutdown due to error or interruption
* Handling changes in the number of shards (resharding) while running
* Correct handling of Kinesis resource limits (throttling and backoff)
* KCL compatible metrics publishing to CloudWatch
* Compatibility for running alongside KCL consumers
* Emission of diagnostic events for custom logging / metrics / testing
* Manual or otherwise user-defined shard assignment strategy
* Pluggable lease/checkpoint storage backend
* Optimized startup + shutdown sequence


### Basic usage

The following example shows a simple ZIO App that consumes from a stream, prints a record and periodically checkpoints.

```scala
import nl.vroste.zio.kinesis.client._
import nl.vroste.zio.kinesis.client.zionative._
import nl.vroste.zio.kinesis.client.serde.Serde
import zio._
import zio.console.{ putStrLn, Console }
import zio.duration._
import zio.logging.Logging
import zio.logging.slf4j.Slf4jLogger

object BasicUsage extends zio.App {
  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    Consumer
      .shardedStream(
        streamName = "my-stream",
        applicationName = "my-application",
        deserializer = Serde.asciiString,
        workerIdentifier = "worker1"
      )
      .flatMapPar(Int.MaxValue) {
        case (shardId, shardStream, checkpointer) =>
          shardStream
            .tap(record => putStrLn(s"Processing record ${record} on shard ${shardId}"))
            .tap(checkpointer.stage(_))
            .via(checkpointer.checkpointBatched[Console](nr = 1000, interval = 5.second))
      }
      .runDrain
      .provideCustomLayer(Consumer.defaultEnvironment ++ Slf4jLogger.make((_, logEntry) => logEntry, Some(getClass.getName)))
      .exitCode

  }

```

Let's go over some particulars of this example:
* `Consumer.shardedStream` is a stream of streams. Each of the inner stream represents a Kinesis Shard. Along with this inner stream, the shard ID and a `Checkpointer` are emitted. Each of the shard streams are processed in parallel. 
* The `deserializer` parameter takes care of deserializing records from bytes to a data type of your choice, in this case an ASCII string. You can easily define custom (de)serializers for, 
for example, JSON data using a JSON library of your choice.
* After processing the record (here: printing to the console), the record is staged for checkpointing. This is useful to ensure that when the shard stream is interrupted or fails, a checkpoint call is made. 
* The `checkpointBatched` method on the `Checkpointer` is a helper method that batches records up to 1000 or within 5 seconds, whichever comes earlier. At the end of that window, the last staged record will be checkpointed. It also takes care of ending the shard stream when the lease has been lost or there is some other error in checkpointing. The `Console` type parameter is unfortunately necessary for correct type inference.
* `runDrain` will run the stream until the application is interrupted or until the stream fails.
* `provideCustomLayer` provides the environment (dependencies via [ZLayer](https://zio.dev/docs/howto/howto_use_layers)) necessary to run the `Consumer`. The default environment uses AWS SDK default settings (i.e. default credential provider)
* `exitCode` maps the failure or success of the stream to a system exit code. 
* A logging environment is provided for debug logging. See [zio-logging](https://github.com/zio/zio-logging) for more information on how to customize this.

### Checkpointing

Checkpointing is a mechanism that ensures that stream processing can be resumed correctly after application restart. Checkpointing has 'up to and including' semantics, which means that checkpointing records with sequence number 100 means acknowledging processing of records 1 to 100. 

It is [recommended](https://github.com/awslabs/amazon-kinesis-client/blob/master/amazon-kinesis-client/src/main/java/software/amazon/kinesis/processor/RecordProcessorCheckpointer.java#L35)
not to checkpoint too frequently. It depends on your application and stream volume what is a good checkpoint frequency 
(in terms of number of records and/or interval). 

`zio-kinesis` has some mechanisms to improve checkpointing safety in the case of interruption or failures:

* To guarantee that the last processed record is checkpointed when the stream shuts down, because of failure or interruption 
for example, checkpoints for every record should be staged by calling `checkpointer.stage(record)`. A periodic call to 
`checkpointer.checkpoint` will 'flush' the last staged checkpoint. 

* To ensure that processing of a record is always followed by a checkpoint stage, even in the face of fiber interruption, 
use the utility method `Checkpointer.stageOnSuccess(processingEffect)(r)`. 

* When checkpointing fails, it is retried according to the provided Schedule. By default this is an exponential backoff schedule with a maximum number of attempts.

* Taking into account retries, checkpointing may fail due to:
  * `ShardLeaseLost`: this indicates that while attempting to update the lease, it was discovered that another worker has stolen the lease. The shard should no longer be processed. This can be achieved by recovering the stream with a `ZStream.empty` via `catchAll`.
  * Transient connection / AWS issues. `Checkpointer.checkpoint` will fail with a `Throwable`. 
  
  
### Lease coordination

`Consumer` supports checkpointing and lease coordination between multiple workers. Leases and checkpoints for each shard are stored in a table (DynamoDB by default). These leases are periodically renewed, unless they were recently (less than the renewal interval) checkpointed already. 

When a new worker joins, it will take some leases from other workers so that all workers get a fair share. The other workers will notice this either during checkpointing or lease renewal and will end their processing of the stream. Because of this, records are processed 'at least once'; for a brief time (at most the renew interval) two workers may be processing records from the same shard. When your application has multiple workers, it must be able to handle records being processed more than once because of this.

When one worker fails or loses connectivity, the other workers will detect that the lease has not been updated for some time and will take over some of its leases, so that all workers have a fair share again.

When a worker is stopped, its leases are released so that other workers may pick them up.

### Resharding

Changing the stream's shard count, or _resharding_, is fully supported while the consumer is active. When a shard is split or two shards are merged, before processing of the new shard starts, the parent shard(s) are processed until the end. When a worker detects the end of a shard it is processing, Kinesis will tell it the new (child) shards and their processing will start immediately after both parent shards have been completely processed. 

When another worker is processing one of the parent shards, it may take a while for this to be detected.

To ensure that no shard is left behind, the list of shards is refreshed periodically.

### Consuming multiple Kinesis streams

If you want to process more than one Kinesis stream, simply create more than one instance and ensure that the application name is unique per stream. The application name is used to create a lease table. Unique application names will create a lease table per stream, to ensure that shard IDs do not conflict. Unlike the KCL, which heavily uses threads, there should be no performance need to have multi-stream support built into `Consumer`.

For example, if your application name is `"order_processing"` and you want to consume the streams `"orders"` and `"payments`", create one `Consumer` with the application name `"order_processing_orders"` and one `"order_processing_payments"`.

### Customization

The following parameters can be customized:

* Initial position  
  When no checkpoint is found for a shard, start from this position in the stream. The default is to start with the oldest message on each shard (`TRIM_HORIZON`). 
* Use enhanced fanout or polling  
  See the [AWS docs](https://aws.amazon.com/blogs/aws/kds-enhanced-fanout/)
* Polling:
    * Maximum batch size
    * Poll interval
    * Backoff schedule in case of throttling (Kinesis limits)
    * Retry schedule in case of other issues
* Enhanced fanout:
    * Maximum shard subscriptions to make per second.  
      Although there is no Kinesis limit, this prevents a stream of 1000 shards from making 1000 simultaneous HTTP requests.
    * Retry schedule in case of issues
* Lease coordination
    * Lease expiration time
    * Lease renewal interval
    * Lease refresh & take interval
    
### Diagnostic event & metrics

`Consumer.shardedStream` has a parameter for a function that is called with a `DiagnosticEvent` whenever something of interest happens in the Consumer. This is useful for diagnostic purposes and for metrics.

A KCL compatible CloudWatch metrics publisher is included, which can optionally be hooked on to these diagnostic events.

See `src/test/nl/vroste/zio/kinesis/client/examples/NativeConsumerWithMetricsExample.scala` for an example.

### KCL compatibility
Lease coordination and metrics are fully compatible for running along other KCL workers.

### Unsupported features

Features that are supported by `DynamicConsumer` but not by `Consumer`:
* KPL record aggregation via Protobuf + subsequence number checkpointing  
  Users can manually deserialize records via Protobuf if desired. Kinesis streams has a at-least once model anyway, so the lack of subsequence number checkpointing does not break that.
* DynamoDB lease table billing mode configuration  
  This can be adjusted in AWS Console if desired or manually using the AWS DynamoDB SDK.
* Some metrics  
  Not all metrics published by the KCL are implemented yet. Some of them are not applicable because of different implementations.

## Configuration
The default environments for `Client`, `AdminClient`, `Consumer`, `DynamicConsumer` and `Producer` will use the [Default Credential/Region Provider](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html).
Using the client builders, many parameters can be customized. Refer to the AWS documentation for more information on the possible parameters.

Default client ZLayers are provided in `nl.vroste.zio.kinesis.client` package object for production and for integration tests are provided in `LocalStackServices`

## Producer
The low-level `Client` offers a `putRecords` method to put records on Kinesis. Although simple to use for a small number of records, 
there are many catches when it comes to efficiently and reliably producing a high volume of records. 

`Producer` helps you achieve high throughput by batching records and respecting the Kinesis request (rate) limits. Records 
that cannot be produced due to temporary errors, like shard rate limits, will be retried.

All of this of course with the robust failure handling you can expect from a ZIO-based library.

Usage example:

```scala
import nl.vroste.zio.kinesis.client
import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.{ Client, Producer }
import zio._
import zio.console.putStrLn
import zio.logging.slf4j.Slf4jLogger

object ProducerExample extends zio.App {
  val streamName      = "my_stream"
  val applicationName = "my_awesome_zio_application"

  val loggingLayer = Slf4jLogger.make((_, logEntry) => logEntry, Some(getClass.getName))
  val env          = client.defaultAwsLayer >+> Client.live ++ loggingLayer

  val program = Producer.make(streamName, Serde.asciiString).use { producer =>
    val record = ProducerRecord("key1", "message1")

    for {
      _ <- producer.produce(record)
      r <- putStrLn(s"All records in the chunk were produced")
    } yield r
  }

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    program.provideCustomLayer(env).exitCode
}
```

### Metrics
`Producer` periodically collects metrics like success rate and throughput and makes them available as `ProducerMetrics` values. Statistical values are collected in a `HdrHistogram`.  Metrics are collected every 30 seconds by default, but the interval can be customized. 

`ProducerMetrics` objects can be combined with other `ProducerMetrics` to get (statistically sound!) total metrics, allowing you to do your own filtering, aggregation or other processing if desired.

```scala
import nl.vroste.zio.kinesis.client
import nl.vroste.zio.kinesis.client.Client.ProducerRecord
import nl.vroste.zio.kinesis.client.serde.Serde
import nl.vroste.zio.kinesis.client.{ Client, Producer, ProducerMetrics, ProducerSettings }
import zio._
import zio.console.putStrLn
import zio.logging.slf4j.Slf4jLogger

object ProducerWithMetricsExample extends zio.App {
  val streamName      = "my_stream"
  val applicationName = "my_awesome_zio_application"

  val loggingLayer = Slf4jLogger.make((_, logEntry) => logEntry, Some(getClass.getName))
  val env          = client.defaultAwsLayer >+> Client.live ++ loggingLayer

  val program = (for {
    totalMetrics <- Ref.make(ProducerMetrics.empty).toManaged_
    producer     <- Producer
                  .make(
                    streamName,
                    Serde.asciiString,
                    ProducerSettings(),
                    metrics => totalMetrics.updateAndGet(_ + metrics).flatMap(m => putStrLn(m.toString))
                  )
  } yield (producer, totalMetrics)).use {
    case (producer, totalMetrics) =>
      val records = (1 to 100).map(j => ProducerRecord(s"key${j}", s"message${j}"))

      for {
        _ <- producer.produceChunk(Chunk.fromIterable(records))
        r <- putStrLn(s"All records in the chunk were produced")
        m <- totalMetrics.get
        _ <- putStrLn(s"Metrics after producing: ${m}")
      } yield ()
  }

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    program.provideCustomLayer(env).exitCode
}

```

## DynamicConsumer
`DynamicConsumer` is an alternative to `Consumer`, backed by the 
[Kinesis Client Library (KCL)](https://docs.aws.amazon.com/streams/latest/dev/shared-throughput-kcl-consumers.html). 

_NOTE: Although `DynamicConsumer` will be included in this library for some time to come for backwards compatibility, it will eventually be deprecated and removed in favour of the ZIO native `Consumer`. Users are recommended to upgrade._
  
The interface is largely the same as `Consumer`, except for:
 * Some parameters for configuration 
 * The ZIO environment
 * Checkpointing is an effect that can fail with a `Throwable` instead of `Either[Throwable, ShardLeaseLost.type]`. A `ShutdownException` indicates that the shard should no longer be processed. See the documentation on `Checkpointer.checkpoint` for more details.
 * Retrying is not done automatically by DynamicConsumer's Checkpointer.

Unlike `Consumer`, `DynamicConsumer` also supports:
* KPL record aggregation via Protobuf + subsequence number checkpointing
* Full CloudWatch metrics publishing

### Basic usage using `consumeWith`
For a lot of use cases where you just want to do something with all messages on a Kinesis stream, `zio-kinesis` provides the 
convenience method `DynamicConsumer.consumeWith`. This method lets you execute a ZIO effect for each message. It takes
care of checkpointing which you can configure through `checkpointBatchSize` and `checkpointDuration` parameters.   

```scala
import nl.vroste.zio.kinesis.client._
import nl.vroste.zio.kinesis.client.DynamicConsumer
import nl.vroste.zio.kinesis.client.serde.Serde
import zio._
import zio.console.putStrLn
import zio.duration._
import zio.logging.slf4j.Slf4jLogger

/**
 * Basic usage example for `DynamicConsumer.consumeWith` convenience method
 */
object DynamicConsumerConsumeWithExample extends zio.App {
  private val loggingLayer = Slf4jLogger.make((_, logEntry) => logEntry, Some(getClass.getName))

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    DynamicConsumer
      .consumeWith(
        streamName = "my-stream",
        applicationName = "my-application",
        deserializer = Serde.asciiString,
        workerIdentifier = "worker1",
        checkpointBatchSize = 1000L,
        checkpointDuration = 5.minutes
      ){ record => // Effectfully process your record here
        putStrLn(s"Processing record $record")
      }
      .provideCustomLayer(loggingLayer ++ defaultAwsLayer >>> DynamicConsumer.live ++ loggingLayer)
      .exitCode
}
``` 
  
### Advanced usage
If you want more control over your stream, `DynamicConsumer.shardedStream` can be used:


```scala
import nl.vroste.zio.kinesis.client._
import nl.vroste.zio.kinesis.client.serde.Serde
import zio._
import zio.blocking.Blocking
import zio.console.{ putStrLn, Console }
import zio.duration._
import zio.logging.slf4j.Slf4jLogger

/**
 * Basic usage example for DynamicConsumer
 */
object DynamicConsumerBasicUsageExample extends zio.App {
  private val loggingLayer = Slf4jLogger.make((_, logEntry) => logEntry, Some(getClass.getName))

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    DynamicConsumer
      .shardedStream(
        streamName = "my-stream",
        applicationName = "my-application",
        deserializer = Serde.asciiString,
        workerIdentifier = "worker1"
      )
      .flatMapPar(Int.MaxValue) {
        case (shardId, shardStream, checkpointer) =>
          shardStream
            .tap(record => putStrLn(s"Processing record ${record} on shard ${shardId}"))
            .tap(checkpointer.stage(_))
            .via(checkpointer.checkpointBatched[Blocking with Console](nr = 1000, interval = 5.second))
      }
      .runDrain
      .provideCustomLayer(loggingLayer ++ defaultAwsLayer >>> DynamicConsumer.live)
      .exitCode
}
```

DynamicConsumer is built on `ZManaged` and therefore resource-safe: after stream completion all resources acquired will be shutdown.


## Client and AdminClient
More low-level operations for consuming, producing and administering streams are available in `Client` and `AdminClient`.

Refer to the [AWS Kinesis Streams API Reference](https://docs.aws.amazon.com/kinesis/latest/APIReference/Welcome.html) 
for more information.

## Running tests and more usage examples 

See [src/test/nl/vroste/zio/kinesis/client/examples](src/test/nl/vroste/zio/kinesis/client/examples) for some examples. The [integration tests](src/test/scala/nl/vroste/zio/kinesis/client) are also good usage examples.

The tests run against a [`localstack`](https://github.com/localstack/localstack) docker image to access 
`kinesis`, `dynamoDb` and `cloudwatch` endpoints locally. In order to run the tests you need to have `docker` and `docker-compose` 
installed on your machine. Then on your machine open a terminal window and navigate to the root of this project and type: 

    > docker-compose -f docker/docker-compose.yml up -d
    
To run the tests, enter the following in the terminal:

    > sbt test   
    
Don't forget to shut down the docker container after you have finished. In the terminal type:     

    > docker-compose -f docker/docker-compose.yml down

## Credits

The Serde construct in this library is inspired by [zio-kafka](https://github.com/zio/zio-kafka), the producer by
 [this AWS blog post](https://aws.amazon.com/blogs/big-data/implementing-efficient-and-reliable-producers-with-the-amazon-kinesis-producer-library/)
 
Table of contents generated with [markdown-toc](http://ecotrust-canada.github.io/markdown-toc/).

