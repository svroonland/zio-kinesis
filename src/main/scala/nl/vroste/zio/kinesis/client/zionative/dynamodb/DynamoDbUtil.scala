package nl.vroste.zio.kinesis.client.zionative.dynamodb

import nl.vroste.zio.kinesis.client.DynamicConsumer.{ Checkpointer, Record }
import nl.vroste.zio.kinesis.client.Util.asZIO
import nl.vroste.zio.kinesis.client.zionative.LeaseCoordinator
import nl.vroste.zio.kinesis.client.zionative.dynamodb.DynamoDbLeaseCoordinator.Lease
import nl.vroste.zio.kinesis.client.zionative.dynamodb.DynamoDbUtil._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._
import software.amazon.awssdk.services.kinesis.model.Shard
import zio._
import zio.clock.Clock
import zio.duration._

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.Try

object DynamoDbUtil {
  type DynamoDbItem = scala.collection.mutable.Map[String, AttributeValue]

  object DynamoDbItem {
    def apply(items: (String, AttributeValue)*): DynamoDbItem = scala.collection.mutable.Map(items: _*)
    val empty                                                 = apply()
  }

  def expectedAttributeValue[T: ClassTag](v: T) = ExpectedAttributeValue.builder().value(attributeValue(v)).build()

  def putAttributeValueUpdate[T: ClassTag](value: T): AttributeValueUpdate =
    AttributeValueUpdate
      .builder()
      .action(AttributeAction.PUT)
      .value(attributeValue(value))
      .build()

  def deleteAttributeValueUpdate: AttributeValueUpdate =
    AttributeValueUpdate
      .builder()
      .action(AttributeAction.DELETE)
      .value(null.asInstanceOf[AttributeValue])
      .build()

  object ImplicitConversions {
    implicit def toAttributeValue[T: ClassTag](value: T): AttributeValue =
      attributeValue[T](value)
  }

  def attributeValue[T: ClassTag](value: T): AttributeValue = {
    val builder = AttributeValue.builder()
    value match {
      case null            => builder.nul(true)
      case v: String       => builder.s(v)
      case v: Long         => builder.n(v.toString)
      case v: List[String] => builder.ss(v.asJavaCollection)
      case v: Seq[String]  => builder.ss(v.asJavaCollection)
      case v               =>
        throw new Exception(s"Could not convert value ${v} to attribute!")
    }
    builder.build()
  }

  def keySchemaElement(name: String, keyType: KeyType)                 =
    KeySchemaElement.builder().attributeName(name).keyType(keyType).build()
  def attributeDefinition(name: String, attrType: ScalarAttributeType) =
    AttributeDefinition.builder().attributeName(name).attributeType(attrType).build()

}
