package nl.vroste.zio.kinesis.client.producer
import java.nio.charset.StandardCharsets
import java.time.Instant

import nl.vroste.zio.kinesis.client.producer.ProducerLive.{ PartitionKey, ShardId }
import software.amazon.awssdk.services.kinesis.model.{ PutRecordsRequestEntry, Shard }
import software.amazon.awssdk.utils.Md5Utils
import zio.Chunk

private[client] case class ShardMap(
  shards: Iterable[(ShardId, BigInt, BigInt)],
  lastUpdated: Instant,
  invalid: Boolean = false
) {
  def shardForPutRecordsRequestEntry(e: PutRecordsRequestEntry): ShardId =
    shardForPartitionKey(Option(e.explicitHashKey()).getOrElse(e.partitionKey()))

  def shardForPartitionKey(key: PartitionKey): ShardId = {
    val hashBytes = Md5Utils.computeMD5Hash(key.getBytes(StandardCharsets.US_ASCII))
    val hashInt   = BigInt.apply(1, hashBytes)

    shards.collectFirst {
      case (shardId, minHashKey, maxHashKey) if hashInt >= minHashKey && hashInt <= maxHashKey => shardId
    }.getOrElse(throw new IllegalArgumentException(s"Could not find shard for partition key ${key}"))
  }

  def invalidate: ShardMap = copy(invalid = true)
}

private[client] object ShardMap {
  val minHashKey: BigInt = BigInt(0)
  val maxHashKey: BigInt = BigInt("340282366920938463463374607431768211455")

  def fromShards(shards: Chunk[Shard], now: Instant): ShardMap = {
    if (shards.isEmpty) throw new IllegalArgumentException("Cannot create ShardMap from empty shards list")
    ShardMap(
      shards
        .map(s => (s.shardId(), BigInt(s.hashKeyRange().startingHashKey()), BigInt(s.hashKeyRange().endingHashKey())))
        .sortBy(_._2),
      now
    )
  }
}
