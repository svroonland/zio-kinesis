package nl.vroste.zio.kinesis.client.zionative

import nl.vroste.zio.kinesis.client.zionative.Consumer.InitialPosition
import nl.vroste.zio.kinesis.client.zionative.LeaseRepository.Lease
import nl.vroste.zio.kinesis.client.zionative.leasecoordinator.DefaultLeaseCoordinator
import software.amazon.awssdk.services.kinesis.model.{ Shard => SdkShard }
import io.github.vigoo.zioaws.kinesis.model.{ HashKeyRange, SequenceNumberRange, Shard }
import zio.test.Assertion._
import zio.test._

object LeaseCoordinatorTest extends DefaultRunnableSpec {

  val shard1 =
    Shard("001", hashKeyRange = HashKeyRange("0", "0"), sequenceNumberRange = SequenceNumberRange("1")).asReadOnly
  val shard2 =
    Shard("002", hashKeyRange = HashKeyRange("0", "0"), sequenceNumberRange = SequenceNumberRange("1")).asReadOnly
  val shard3 = Shard(
    "003",
    parentShardId = Some("001"),
    adjacentParentShardId = Some("002"),
    hashKeyRange = HashKeyRange("0", "0"),
    sequenceNumberRange = SequenceNumberRange("1")
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
          val shards = Seq(shard1, shard2).map(s => s.shardIdValue -> s).toMap

          val leases: Map[String, Lease] = Map.empty

          val readyToConsume = DefaultLeaseCoordinator.shardsReadyToConsume(shards, leases).keySet

          assert(readyToConsume)(equalTo(Set("001", "002")))
        },
        test("shards without parents and open leases ready to consume") {
          val shards = Seq(shard1, shard2).map(s => s.shardIdValue -> s).toMap

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

          val readyToConsume = DefaultLeaseCoordinator.shardsReadyToConsume(shards, leases).keySet

          assert(readyToConsume)(equalTo(Set("001", "002")))
        },
        test("shards without parents and ended leases not ready to consume") {
          val shards = Seq(shard1, shard2).map(s => s.shardIdValue -> s).toMap

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

          val readyToConsume = DefaultLeaseCoordinator.shardsReadyToConsume(shards, leases).keySet

          assert(readyToConsume)(equalTo(Set.empty[String]))
        },
        test("shards with open parents but no leases not ready to consume") {
          val shards = Seq(shard1, shard2, shard3).map(s => s.shardIdValue -> s).toMap

          val leases: Map[String, Lease] = Map.empty

          val readyToConsume = DefaultLeaseCoordinator.shardsReadyToConsume(shards, leases).keySet

          assert(readyToConsume)(not(contains("003")))
        },
        test("shards with expired parents ready to consume") {
          val shards = Seq(shard3).map(s => s.shardIdValue -> s).toMap

          val leases: Map[String, Lease] = Map.empty

          val readyToConsume = DefaultLeaseCoordinator.shardsReadyToConsume(shards, leases).keySet

          assert(readyToConsume)(contains("003"))
        },
        test("shards with open parents and open leases not ready to consume") {
          val shards = Seq(shard1, shard2, shard3).map(s => s.shardIdValue -> s).toMap

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

          val readyToConsume = DefaultLeaseCoordinator.shardsReadyToConsume(shards, leases).keySet

          assert(readyToConsume)(not(contains("003")))
        },
        test("shards with two parents of which one has expired not ready to consume") {
          val shards = Seq(shard2, shard3).map(s => s.shardIdValue -> s).toMap

          val leases: Map[String, Lease] = Map.empty

          val readyToConsume = DefaultLeaseCoordinator.shardsReadyToConsume(shards, leases).keySet

          assert(readyToConsume)(not(contains("003")))
        }
      )
    )
}
