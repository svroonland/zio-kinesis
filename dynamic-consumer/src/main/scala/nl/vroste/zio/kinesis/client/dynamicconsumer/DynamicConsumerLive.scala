package nl.vroste.zio.kinesis.client.dynamicconsumer

import nl.vroste.zio.kinesis.client.serde.Deserializer
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.kinesis.common.{ ConfigsBuilder, InitialPositionInStreamExtended }
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.exceptions.ShutdownException
import software.amazon.kinesis.lifecycle.events._
import software.amazon.kinesis.processor.{
  RecordProcessorCheckpointer,
  ShardRecordProcessor,
  ShardRecordProcessorFactory
}
import software.amazon.kinesis.retrieval.KinesisClientRecord
import zio._
import zio.blocking.Blocking
import zio.logging.Logger
import zio.stream.ZStream

import scala.jdk.CollectionConverters._

private[client] class DynamicConsumerLive(
  logger: Logger[String],
  kinesisAsyncClient: KinesisAsyncClient,
  cloudWatchAsyncClient: CloudWatchAsyncClient,
  dynamoDbAsyncClient: DynamoDbAsyncClient
) extends DynamicConsumer.Service {
  override def shardedStream[R, T](
    streamName: String,
    applicationName: String,
    deserializer: Deserializer[R, T],
    requestShutdown: UIO[Unit],
    initialPosition: InitialPositionInStreamExtended,
    leaseTableName: Option[String] = None,
    metricsNamespace: Option[String] = None,
    workerIdentifier: String,
    maxShardBufferSize: Int,
    configureKcl: SchedulerConfig => SchedulerConfig
  ): ZStream[
    Blocking with R,
    Throwable,
    (String, ZStream[Blocking, Throwable, DynamicConsumer.Record[T]], DynamicConsumer.Checkpointer)
  ] = {
    sealed trait ShardQueueStopReason
    object ShardQueueStopReason {
      case object ShutdownRequested extends ShardQueueStopReason
      case object ShardEnded        extends ShardQueueStopReason
      case object LeaseLost         extends ShardQueueStopReason
    }

    /*
     * A queue for a single Shard and interface between the KCL threadpool and the ZIO runtime
     *
     * This queue is used by a ZStream for a single Shard
     *
     * The Queue uses the error channel (E type parameter) to signal failure (Some[Throwable])
     * and completion (None)
     */
    class ShardQueue(
      val shardId: String,
      runtime: zio.Runtime[Any],
      val q: Queue[Exit[Option[Throwable], KinesisClientRecord]],
      checkpointerInternal: CheckpointerInternal
    ) {
      def offerRecords(r: java.util.List[KinesisClientRecord]): Unit =
        // Calls to q.offer will fail with an interruption error after the queue has been shutdown
        // We must make sure never to throw an exception here, because KCL will consider the records processed
        // See https://github.com/awslabs/amazon-kinesis-client/issues/10
        runtime.unsafeRun {
          val records = r.asScala
          for {
            _ <- logger.debug(s"offerRecords for ${shardId} got ${records.size} records")
            _ <- checkpointerInternal.setMaxSequenceNumber(
                   ExtendedSequenceNumber(
                     records.last.sequenceNumber(),
                     Option(records.last.subSequenceNumber()).filter(_ != 0L)
                   )
                 )
            _ <- q.offerAll(records.map(Exit.succeed)).unit.catchSomeCause { case c if c.interrupted => ZIO.unit }
            _ <- logger.trace(s"offerRecords for ${shardId} COMPLETE")
          } yield ()

          // TODO what behavior do we want if the queue + substream are already shutdown for some reason..?
        }

      def shutdownQueue: UIO[Unit] =
        logger.debug(s"shutdownQueue for ${shardId}") *>
          q.shutdown

      /**
       * Shutdown processing for this shard
       *
       * Clear everything that is still in the queue, offer a completion signal for the queue, set an interrupt signal
       * and await stream completion (in-flight messages processed)
       */
      def stop(reason: ShardQueueStopReason): Unit =
        runtime.unsafeRun {
          // Clear the queue so it doesn't have to be drained fully
          def drainQueueUnlessShardEnded =
            q.takeAll.unit.unless(reason == ShardQueueStopReason.ShardEnded)

          for {
            _ <- logger.debug(s"stop() for ${shardId} because of ${reason}")
            _ <- checkpointerInternal.markEndOfShard.when(reason == ShardQueueStopReason.ShardEnded)
            _ <- (drainQueueUnlessShardEnded *>
                   q.offer(Exit.fail(None)).unit <* // Pass an exit signal in the queue to stop the stream
                   q.awaitShutdown).race(q.awaitShutdown)
            _ <- logger.trace(s"stop() for ${shardId} because of ${reason} - COMPLETE")
          } yield ()

          // TODO maybe we want to only do this when the main stream's completion has bubbled up..?
        }
    }

    class ZioShardProcessorFactory(queues: Queues) extends ShardRecordProcessorFactory {
      override def shardRecordProcessor(): ShardRecordProcessor = new ZioShardProcessor(queues)
    }

    class ZioShardProcessor(queues: Queues) extends ShardRecordProcessor {
      var shardId: Option[String]        = None
      var shardQueue: Option[ShardQueue] = None

      override def initialize(input: InitializationInput): Unit =
        shardId = Some(input.shardId())

      override def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
        if (shardQueue.isEmpty)
          shardQueue = shardId.map(shardId => queues.newShard(shardId, processRecordsInput.checkpointer()))

        shardQueue.foreach(_.offerRecords(processRecordsInput.records()))
      }

      override def leaseLost(leaseLostInput: LeaseLostInput): Unit =
        shardQueue.foreach(_.stop(ShardQueueStopReason.LeaseLost))

      override def shardEnded(shardEndedInput: ShardEndedInput): Unit = {
        shardQueue.foreach(_.stop(ShardQueueStopReason.ShardEnded))
        shardEndedInput.checkpointer().checkpoint()
      }

      override def shutdownRequested(shutdownRequestedInput: ShutdownRequestedInput): Unit =
        shardQueue.foreach(_.stop(ShardQueueStopReason.ShutdownRequested))
    }

    class Queues(
      private val runtime: zio.Runtime[Any],
      val shards: Queue[Exit[Option[Throwable], (String, ShardQueue, CheckpointerInternal)]]
    ) {
      def newShard(shard: String, checkpointer: RecordProcessorCheckpointer): ShardQueue =
        runtime.unsafeRun {
          for {
            checkpointer <- Checkpointer.make(checkpointer, logger)
            queue        <- Queue
                              .bounded[Exit[Option[Throwable], KinesisClientRecord]](
                                maxShardBufferSize
                              )
                              .map(new ShardQueue(shard, runtime, _, checkpointer))
            _            <- shards.offer(Exit.succeed((shard, queue, checkpointer))).unit
          } yield queue
        }

      def shutdown: UIO[Unit] =
        shards.offer(Exit.fail(None)).unit
    }

    object Queues {
      def make: ZManaged[Any, Nothing, Queues] =
        for {
          runtime <- ZIO.runtime[Any].toManaged_
          q       <-
            Queue.unbounded[Exit[Option[Throwable], (String, ShardQueue, CheckpointerInternal)]].toManaged(_.shutdown)
        } yield new Queues(runtime, q)
    }

    def toRecord(
      shardId: String,
      r: KinesisClientRecord
    ): ZIO[R, Throwable, DynamicConsumer.Record[T]] =
      deserializer.deserialize(Chunk.fromByteBuffer(r.data())).map { data =>
        DynamicConsumer.Record(
          shardId,
          r.sequenceNumber(),
          r.approximateArrivalTimestamp(),
          data,
          r.partitionKey(),
          r.encryptionType(),
          Option(r.subSequenceNumber()).filterNot(_ == 0L),
          Option(r.explicitHashKey()).filterNot(_.isEmpty),
          r.aggregated()
        )
      }

    // Run the scheduler
    val schedulerM =
      for {
        queues <- Queues.make

        configsBuilder = {
          val configsBuilder = new ConfigsBuilder(
            streamName,
            applicationName,
            kinesisAsyncClient,
            dynamoDbAsyncClient,
            cloudWatchAsyncClient,
            workerIdentifier,
            new ZioShardProcessorFactory(queues)
          )
          leaseTableName.fold(configsBuilder)(configsBuilder.tableName)
          metricsNamespace.fold(configsBuilder)(configsBuilder.namespace)
        }
        config         = configureKcl(
                           SchedulerConfig.makeDefault(configsBuilder, kinesisAsyncClient, initialPosition, streamName)
                         )
        env           <- ZIO.environment[R].toManaged_

        scheduler <- Task(
                       new Scheduler(
                         config.checkpoint,
                         config.coordinator,
                         config.leaseManagement,
                         config.lifecycle,
                         config.metrics,
                         config.processor,
                         config.retrieval
                       )
                     ).toManaged_
        doShutdown = logger.debug("Starting graceful shutdown") *>
                       ZIO.fromFutureJava(scheduler.startGracefulShutdown()).unit.orDie <*
                       queues.shutdown
        _         <- zio.blocking
                       .blocking(ZIO(scheduler.run()))
                       .fork
                       .flatMap(_.join)
                       .onInterrupt(doShutdown)
                       .forkManaged
        _         <- (requestShutdown *> doShutdown).forkManaged
      } yield ZStream
        .fromQueue(queues.shards)
        .flattenExitOption
        .map { case (shardId, shardQueue, checkpointer) =>
          val stream = ZStream
            .fromQueue(shardQueue.q)
            .ensuringFirst(shardQueue.shutdownQueue)
            .flattenExitOption
            .mapChunksM(_.mapM(toRecord(shardId, _)))
            .provide(env)
            .ensuringFirst((checkpointer.checkEndOfShardCheckpointed *> checkpointer.checkpoint).catchSome {
              case _: ShutdownException => UIO.unit
            }.orDie)

          (shardId, stream, checkpointer)
        }

    ZStream.unwrapManaged(schedulerM)

  }
}
