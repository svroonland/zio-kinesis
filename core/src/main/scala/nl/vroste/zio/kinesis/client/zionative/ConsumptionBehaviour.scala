package nl.vroste.zio.kinesis.client.zionative

import zio._
import nl.vroste.zio.kinesis.client.Util
import zio.stream.ZStream
import nl.vroste.zio.kinesis.client.Record

trait ConsumptionBehaviour[-R] {
  def processShardStream[RC <: R, T](
    shardStream: ZStream[Any, Throwable, Record[T]],
    checkpointer: Checkpointer,
    processRecord: Record[T] => RIO[RC, Unit]
  ): ZIO[RC, Throwable, Unit]
}

object ConsumptionBehaviour {

  /**
   * Default consumption behaviour. No special guarantees are made about ordering or checkpointing.
   *
   * @param checkpointBatchSize
   *   Maximum number of records before checkpointing
   * @param checkpointDuration
   *   Maximum interval before checkpointing
   * @param processorParallelism
   *   Number of fibers to process records in parallel per shard
   */
  def default(
    checkpointBatchSize: Long = 200,
    checkpointDuration: Duration = 5.minutes,
    processorParallelism: Int = 1
  ): ConsumptionBehaviour[Any] = new ConsumptionBehaviour[Any] {
    def processShardStream[RC, T](
      shardStream: ZStream[Any, Throwable, Record[T]],
      checkpointer: Checkpointer,
      processRecord: Record[T] => RIO[RC, Unit]
    ): ZIO[RC, Throwable, Unit] =
      shardStream
        .mapZIOPar(processorParallelism)(r => processRecord(r) *> checkpointer.stage(r).as(r))
        .viaFunction(checkpointer.checkpointBatched[RC](nr = checkpointBatchSize, interval = checkpointDuration))
        .runDrain
  }

  /**
   * This ensures that a checkpoint is performed between every 2 records with the same partition key. This is useful
   * when you want to process a shard in paralel but ensure that records with the same partition key are processed in
   * order. Even in the presence of failure, only a at most 1 record per partition key will be reprocessed.
   *
   * @param checkpointBatchSize
   *   Maximum number of records before checkpointing
   * @param checkpointDuration
   *   Maximum interval before checkpointing
   * @param checkpointRetrySchedule
   *   Schedule for retrying checkpointing in case of failure
   * @param processorParallelism
   *   Number of fibers to process records in parallel per shard
   * @return
   *   A ZIO that completes with Unit when record processing is stopped or fails when the consumer stream fails
   */
  def partitionedCheckpoints[R](
    checkpointBatchSize: Long = 200,
    checkpointDuration: Duration = 5.minutes,
    checkpointRetrySchedule: Schedule[R, Throwable, Any] =
      Util.exponentialBackoff(min = 1.second, max = 1.minute, maxRecurs = Some(5)),
    processorParallelism: Int = 1
  ): ConsumptionBehaviour[R] = new ConsumptionBehaviour[R] {
    def processShardStream[RC <: R, T](
      shardStream: ZStream[Any, Throwable, Record[T]],
      checkpointer: Checkpointer,
      processRecord: Record[T] => RIO[RC, Unit]
    ): ZIO[RC, Throwable, Unit] = {
      val makeLockingCheckpointer = LockingCheckpointer.make[T](checkpointer, checkpointBatchSize, checkpointDuration)
      makeLockingCheckpointer.flatMap { lockingCheckpointer =>
        shardStream
          .tap(lockingCheckpointer.lock)
          // Use mapZIOPar even when parallelism is 1 to ensure that locking happens on a separate fiber.
          .mapZIOPar(processorParallelism)(r => processRecord(r).as(r))
          .tap(lockingCheckpointer.stage)
          .mapError(Left(_))
          .merge(ZStream.fromZIO(lockingCheckpointer.checkpointLoop(checkpointRetrySchedule)))
          .catchAll {
            case Left(e)               =>
              ZStream.fail(e)
            case Right(ShardLeaseLost) =>
              ZStream.empty
          }
          .runDrain
      }
    }
  }

}
