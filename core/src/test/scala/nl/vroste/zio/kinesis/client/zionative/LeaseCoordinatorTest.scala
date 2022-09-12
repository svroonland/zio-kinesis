package nl.vroste.zio.kinesis.client.zionative

import nl.vroste.zio.kinesis.client.zionative.Consumer.InitialPosition
import nl.vroste.zio.kinesis.client.zionative.LeaseRepository.Lease
import nl.vroste.zio.kinesis.client.zionative.leasecoordinator.DefaultLeaseCoordinator
import zio.aws.kinesis.model.primitives.{ HashKey, SequenceNumber, ShardId }
import zio.aws.kinesis.model.{ HashKeyRange, SequenceNumberRange, Shard }
import zio.test.Assertion._
import zio.test._

object LeaseCoordinatorTest extends ZIOSpecDefault {
  def toShardMapWithStringKey(shards: Seq[Shard.ReadOnly]): Map[String, Shard.ReadOnly] =
    shards.map(s => ShardId.unwrap(s.shardId) -> s).toMap

  val shard1 =
    Shard(
      ShardId("001"),
      hashKeyRange = HashKeyRange(HashKey("0"), HashKey("0")),
      sequenceNumberRange = SequenceNumberRange(SequenceNumber("1"))
    ).asReadOnly
  val shard2 =
    Shard(
      ShardId("002"),
      hashKeyRange = HashKeyRange(HashKey("0"), HashKey("0")),
      sequenceNumberRange = SequenceNumberRange(SequenceNumber("1"))
    ).asReadOnly
  val shard3 = Shard(
    ShardId("003"),
    parentShardId = Some(ShardId("001")),
    adjacentParentShardId = Some(ShardId("002")),
    hashKeyRange = HashKeyRange(HashKey("0"), HashKey("0")),
    sequenceNumberRange = SequenceNumberRange(SequenceNumber("1"))
  ).asReadOnly

  override def spec =
    suite("DefaultLeaseCoordinator")(
      suite("initialCheckpointForShard")(
        test("for InitialPosition.Latest for shard without parents") {

          val checkpoint = DefaultLeaseCoordinator.initialCheckpointForShard(shard1, InitialPosition.Latest, Map.empty)

          assert(checkpoint)(equalTo(SpecialCheckpoint.Latest))
        },
        test("for InitialPosition.Latest for shard with parents without lease") {
          val checkpoint = DefaultLeaseCoordinator.initialCheckpointForShard(shard3, InitialPosition.Latest, Map.empty)

          assert(checkpoint)(equalTo(SpecialCheckpoint.Latest))
        },
        test("for InitialPosition.Latest for shard with parents with open leases") {
          val leases: Map[String, Lease] = Map(
            "001" -> Lease(
              key = "001",
              owner = Some("worker1"),
              counter = 1,
              checkpoint = Some(Right(ExtendedSequenceNumber("1", 0))),
              parentShardIds = Seq.empty
            )
          )
          val checkpoint                 = DefaultLeaseCoordinator.initialCheckpointForShard(shard3, InitialPosition.Latest, leases)

          assert(checkpoint)(equalTo(SpecialCheckpoint.TrimHorizon))
        },
        test("for InitialPosition.Latest for shard with parents with ended leases") {
          val leases: Map[String, Lease] = Map(
            "001" -> Lease(
              key = "001",
              owner = Some("worker1"),
              counter = 1,
              checkpoint = Some(Left(SpecialCheckpoint.ShardEnd)),
              parentShardIds = Seq.empty
            )
          )
          val checkpoint                 = DefaultLeaseCoordinator.initialCheckpointForShard(shard3, InitialPosition.Latest, leases)

          assert(checkpoint)(equalTo(SpecialCheckpoint.TrimHorizon))
        }
      ),
      suite("shardsReadyToConsume")(
        test("shards without parents and no leases ready to consume") {
          val shards = Seq(shard1, shard2)

          val leases: Map[String, Lease] = Map.empty

          val readyToConsume =
            DefaultLeaseCoordinator.shardsReadyToConsume(toShardMapWithStringKey(shards), leases).keySet

          assert(readyToConsume)(equalTo(Set("001", "002")))
        },
        test("shards without parents and open leases ready to consume") {
          val shards = Seq(shard1, shard2)

          val leases = Map(
            "001" -> Lease(
              key = "001",
              owner = Some("worker1"),
              counter = 1,
              checkpoint = Some(Right(ExtendedSequenceNumber("1", 0))),
              parentShardIds = Seq.empty
            ),
            "002" -> Lease(
              key = "002",
              owner = Some("worker1"),
              counter = 1,
              checkpoint = Some(Right(ExtendedSequenceNumber("1", 0))),
              parentShardIds = Seq.empty
            )
          )

          val readyToConsume =
            DefaultLeaseCoordinator.shardsReadyToConsume(toShardMapWithStringKey(shards), leases).keySet

          assert(readyToConsume)(equalTo(Set("001", "002")))
        },
        test("shards without parents and ended leases not ready to consume") {
          val shards = Seq(shard1, shard2)

          val leases = Map(
            "001" -> Lease(
              key = "001",
              owner = Some("worker1"),
              counter = 1,
              checkpoint = Some(Left(SpecialCheckpoint.ShardEnd)),
              parentShardIds = Seq.empty
            ),
            "002" -> Lease(
              key = "002",
              owner = Some("worker1"),
              counter = 1,
              checkpoint = Some(Left(SpecialCheckpoint.ShardEnd)),
              parentShardIds = Seq.empty
            )
          )

          val readyToConsume =
            DefaultLeaseCoordinator.shardsReadyToConsume(toShardMapWithStringKey(shards), leases).keySet

          assert(readyToConsume)(equalTo(Set.empty[String]))
        },
        test("shards with open parents but no leases not ready to consume") {
          val shards = Seq(shard1, shard2, shard3)

          val leases: Map[String, Lease] = Map.empty

          val readyToConsume =
            DefaultLeaseCoordinator.shardsReadyToConsume(toShardMapWithStringKey(shards), leases).keySet

          assert(readyToConsume)(not(contains("003")))
        },
        test("shards with expired parents ready to consume") {
          val shards = Seq(shard3)

          val leases: Map[String, Lease] = Map.empty

          val readyToConsume =
            DefaultLeaseCoordinator.shardsReadyToConsume(toShardMapWithStringKey(shards), leases).keySet

          assert(readyToConsume)(contains("003"))
        },
        test("shards with open parents and open leases not ready to consume") {
          val shards = Seq(shard1, shard2, shard3)

          val leases = Map(
            "001" -> Lease(
              key = "001",
              owner = Some("worker1"),
              counter = 1,
              checkpoint = Some(Right(ExtendedSequenceNumber("1", 0))),
              parentShardIds = Seq.empty
            ),
            "002" -> Lease(
              key = "002",
              owner = Some("worker1"),
              counter = 1,
              checkpoint = Some(Right(ExtendedSequenceNumber("1", 0))),
              parentShardIds = Seq.empty
            )
          )

          val readyToConsume =
            DefaultLeaseCoordinator.shardsReadyToConsume(toShardMapWithStringKey(shards), leases).keySet

          assert(readyToConsume)(not(contains("003")))
        },
        test("shards with two parents of which one has expired not ready to consume") {
          val shards = Seq(shard2, shard3)

          val leases: Map[String, Lease] = Map.empty

          val readyToConsume =
            DefaultLeaseCoordinator.shardsReadyToConsume(toShardMapWithStringKey(shards), leases).keySet

          assert(readyToConsume)(not(contains("003")))
        }
      )
    )
}
