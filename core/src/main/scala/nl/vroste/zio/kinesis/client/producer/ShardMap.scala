package nl.vroste.zio.kinesis.client.producer
import nl.vroste.zio.kinesis.client.producer.ProducerLive.ShardId
import zio.aws.kinesis.model.Shard
import zio.aws.kinesis.model.primitives.{ HashKey, PartitionKey }
import zio.{ Chunk, Scope, ZIO }

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant

private[client] final case class ShardMap(
  minHashKeys: Chunk[BigInt],
  maxHashKeys: Chunk[BigInt],
  shardIds: Chunk[ShardId],
  lastUpdated: Instant,
  invalid: Boolean = false
) {
  def shardForPartitionKey(digest: MessageDigest, key: PartitionKey): ShardId = {
    val hashBytes = digest.digest(key.getBytes(StandardCharsets.UTF_8))
    val hashInt   = BigInt.apply(1, hashBytes)

    var i                = 0
    val len              = minHashKeys.size
    var shardId: ShardId = null
    while (shardId == null && i < len) {
      val min = minHashKeys(i)
      if (hashInt >= min) {
        val max = maxHashKeys(i)
        if (hashInt <= max)
          shardId = shardIds(i)
      }

      i = i + 1
    }

    if (shardId == null)
      throw new IllegalArgumentException(s"Could not find shard for partition key ${key}")

    shardId
  }

  def invalidate: ShardMap = copy(invalid = true)
}

private[client] object ShardMap {
  val minHashKey: BigInt                        = BigInt(0)
  val maxHashKey: BigInt                        = BigInt("340282366920938463463374607431768211455")
  val md5: ZIO[Scope, Throwable, MessageDigest] = ZIO.attempt(MessageDigest.getInstance("MD5"))

  def fromShards(shards: Chunk[Shard.ReadOnly], now: Instant): ShardMap = {
    if (shards.isEmpty) throw new IllegalArgumentException("Cannot create ShardMap from empty shards list")

    val sorted = shards.sortBy(s => HashKey.unwrap(s.hashKeyRange.startingHashKey))
    ShardMap(
      sorted.map(s => BigInt(s.hashKeyRange.startingHashKey)).materialize,
      sorted.map(s => BigInt(s.hashKeyRange.endingHashKey)).materialize,
      sorted.map(s => s.shardId),
      now
    )
  }
}
