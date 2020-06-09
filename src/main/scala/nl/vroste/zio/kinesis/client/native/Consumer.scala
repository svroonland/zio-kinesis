package nl.vroste.zio.kinesis.client.native

import nl.vroste.zio.kinesis.client.AdminClient.StreamDescription
import nl.vroste.zio.kinesis.client.{ AdminClient, Client }
import nl.vroste.zio.kinesis.client.Client.{ ConsumerRecord, ShardIteratorType }
import nl.vroste.zio.kinesis.client.DynamicConsumer.{ Checkpointer, Record }
import nl.vroste.zio.kinesis.client.native.FetchMode.{ EnhancedFanOut, Polling }
import nl.vroste.zio.kinesis.client.serde.Deserializer
import software.amazon.awssdk.services.kinesis.model.{
  KmsThrottlingException,
  LimitExceededException,
  ProvisionedThroughputExceededException,
  Shard,
  Record => KinesisRecord
}
import zio._
import zio.clock.Clock
import zio.duration._
import zio.stream.ZStream
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement
import software.amazon.awssdk.services.dynamodb.model.KeyType
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest
import software.amazon.awssdk.services.dynamodb.model.TableStatus
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput
import software.amazon.awssdk.services.dynamodb.model.BillingMode
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

sealed trait FetchMode
object FetchMode {

  /**
   * Fetches data in a polling manner
   *
   * @param batchSize The maximum number of records to retrieve in one call to GetRecords. Note that Kinesis
   *        defines limits in terms of the maximum size in bytes of this call, so you need to take into account
   *        the distribution of data size of your records (i.e. avg and max).
   * @param delay How long to wait after polling returned no new records
   * @param backoff When getting a Provisioned Throughput Exception or KmsThrottlingException, schedule to apply for backoff
   */
  case class Polling(
    batchSize: Int = 100,
    delay: Duration = 1.second,
    backoff: Schedule[Clock, Throwable, Any] = Util.exponentialBackoff(1.second, 1.minute)
  ) extends FetchMode

  /**
   * Fetch data using enhanced fanout
   */
  case object EnhancedFanOut extends FetchMode
}

trait Fetcher {
  def fetch(shard: Shard, startingPosition: ShardIteratorType): ZStream[Clock, Throwable, ConsumerRecord]
}

object Fetcher {
  def apply(f: (Shard, ShardIteratorType) => ZStream[Clock, Throwable, ConsumerRecord]): Fetcher =
    (shard, startingPosition) => f(shard, startingPosition)
}

object Consumer {
  def shardedStream[R, T](
    client: Client,
    adminClient: AdminClient,
    dynamoClient: DynamoDbAsyncClient,
    streamName: String,
    applicationName: String,
    deserializer: Deserializer[R, T],
    fetchMode: FetchMode = FetchMode.Polling()
  ): ZStream[Clock, Throwable, (String, ZStream[R with Clock, Throwable, Record[T]], Checkpointer)] = {

    def toRecord(
      shardId: String,
      r: ConsumerRecord
    ): ZIO[R, Throwable, Record[T]] =
      deserializer.deserialize(r.data.asByteBuffer()).map { data =>
        Record(
          shardId,
          r.sequenceNumber,
          r.approximateArrivalTimestamp,
          data,
          r.partitionKey,
          r.encryptionType,
          0,    // r.subSequenceNumber,
          "",   // r.explicitHashKey,
          false //r.aggregated,
        )
      }

    val currentShards: ZStream[Clock, Throwable, Shard] = client.listShards(streamName)

    def makeFetcher(streamDescription: StreamDescription): ZManaged[Clock, Throwable, Fetcher] =
      fetchMode match {
        case c: Polling     => PollingFetcher.make(client, streamDescription, c)
        case EnhancedFanOut => EnhancedFanOutFetcher.make(client, streamDescription, applicationName)
      }

    ZStream.unwrapManaged {
      for {
        streamDescription <- adminClient.describeStream(streamName).toManaged_
        _                 <- DynamoDb.createLeaseTableIfNotExists(dynamoClient, applicationName).toManaged_
        leases            <- DynamoDb.getLeases(dynamoClient, applicationName).toManaged_
        _                 <- UIO(println(s"Found ${leases.size} existing leases: " + leases.mkString("\n"))).toManaged_
        fetcher           <- makeFetcher(streamDescription)
      } yield currentShards.map { shard =>
        val startingPosition = ShardIteratorType.TrimHorizon

        val shardStream = fetcher.fetch(shard, startingPosition).mapChunksM { chunk =>
          chunk.mapM(record => toRecord(shard.shardId(), record))
        }

        (shard.shardId(), shardStream, dummyCheckpointer)
      }
    }
  }

  val dummyCheckpointer = new Checkpointer {
    override def checkpoint: ZIO[zio.blocking.Blocking, Throwable, Unit] = ZIO.unit
    override def stage(r: Record[_]): zio.UIO[Unit]                      = ZIO.unit
  }

  implicit class ZioDebugExtensions[R, E, A](z: ZIO[R, E, A]) {
    def debug(label: String): ZIO[R, E, A] = (UIO(println(s"${label}")) *> z) <* UIO(println(s"${label} complete"))
  }

  def toConsumerRecord(record: KinesisRecord, shardId: String): ConsumerRecord =
    ConsumerRecord(
      record.sequenceNumber(),
      record.approximateArrivalTimestamp(),
      record.data(),
      record.partitionKey(),
      record.encryptionType(),
      shardId
    )

  val isThrottlingException: PartialFunction[Throwable, Unit] = {
    case _: KmsThrottlingException                 => ()
    case _: ProvisionedThroughputExceededException => ()
    case _: LimitExceededException                 => ()
  }

  def retryOnThrottledWithSchedule[R, A](schedule: Schedule[R, Throwable, A]): Schedule[R, Throwable, (Throwable, A)] =
    Schedule.doWhile[Throwable](e => isThrottlingException.lift(e).isDefined) && schedule

}

object DynamoDb {
  import nl.vroste.zio.kinesis.client.Util.asZIO
  import scala.jdk.CollectionConverters._

  case class ExtendedSequenceNumber(sequenceNumber: String, subSequenceNumber: Long)

  case class Lease(
    key: String,
    owner: String,
    counter: Long,
    sequenceNumber: ExtendedSequenceNumber,
    parentShardIds: Seq[String],
    pendingCheckpoint: Option[ExtendedSequenceNumber]
  )

  def createLeaseTableIfNotExists(client: DynamoDbAsyncClient, name: String): ZIO[Clock, Throwable, Unit] =
    createLeaseTable(client, name)
      .unlessM(leaseTableExists(client, name))

  // TODO billing mode
  def createLeaseTable(client: DynamoDbAsyncClient, name: String): ZIO[Clock, Throwable, Unit] = {
    val keySchema            = List(keySchemaElement("leaseKey", KeyType.HASH))
    val attributeDefinitions = List(attributeDefinition("leaseKey", ScalarAttributeType.S))

    val request = CreateTableRequest
      .builder()
      .tableName(name)
      .billingMode(BillingMode.PAY_PER_REQUEST)
      .keySchema(keySchema.asJavaCollection)
      .attributeDefinitions(attributeDefinitions.asJavaCollection)
      .build()

    val createTable = UIO(println("Creating lease table")) *> asZIO(client.createTable(request))

    // recursion, yeah!
    def awaitTableCreated: ZIO[Clock, Throwable, Unit] =
      leaseTableExists(client, name)
        .flatMap(awaitTableCreated.delay(10.seconds).unless(_))

    createTable *>
      awaitTableCreated
        .timeoutFail(new Exception("Timeout creating lease table"))(10.minute) // I dunno
  }

  def leaseTableExists(client: DynamoDbAsyncClient, name: String): Task[Boolean] =
    asZIO(client.describeTable(DescribeTableRequest.builder().tableName(name).build()))
      .map(_.table().tableStatus() == TableStatus.ACTIVE)
      .catchSome { case _: ResourceNotFoundException => ZIO.succeed(false) }
      .tap(exists => UIO(println(s"Lease table ${name} exists? ${exists}")))

  type DynamoDbItem = scala.collection.mutable.Map[String, AttributeValue]

  import nl.vroste.zio.kinesis.client.Util.paginatedRequest
  def getLeases(client: DynamoDbAsyncClient, tableName: String): ZIO[Clock, Throwable, List[Lease]] =
    paginatedRequest { (lastItem: Option[DynamoDbItem]) =>
      val builder     = ScanRequest.builder().tableName(tableName)
      val scanRequest = lastItem.map(_.asJava).fold(builder)(builder.exclusiveStartKey(_)).build()

      asZIO(client.scan(scanRequest)).map { response =>
        val items: Chunk[DynamoDbItem] = Chunk.fromIterable(response.items().asScala).map(_.asScala)

        (items, Option(response.lastEvaluatedKey()).map(_.asScala).filter(_.nonEmpty))
      }

    }(Schedule.forever).flattenChunks.runCollect.map(_.toList.map(toLease))

  private def toLease(item: DynamoDbItem): Lease =
    Lease(
      key = item.get("leaseKey").get.s(),
      owner = item.get("leaseOwner").get.s(),
      counter = item.get("leaseCounter").get.n().toLong,
      sequenceNumber = ExtendedSequenceNumber(
        sequenceNumber = item.get("checkpoint").get.s(),
        subSequenceNumber = item.get("checkpointSubSequenceNumber").get.n().toLong
      ),
      parentShardIds = item.get("parentShardId").get.ss().asScala.toList,
      pendingCheckpoint = item
        .get("pendingCheckpoint")
        .map(_.s())
        .map(ExtendedSequenceNumber(_, item.get("pendingCheckpointSubSequenceNumber").get.n().toLong))
    )

  def updateLease(client: String, applicationName: String, shard: String): Task[Lease] = ???

  def keySchemaElement(name: String, keyType: KeyType)                 =
    KeySchemaElement.builder().attributeName(name).keyType(keyType).build()
  def attributeDefinition(name: String, attrType: ScalarAttributeType) =
    AttributeDefinition.builder().attributeName(name).attributeType(attrType).build()
}
