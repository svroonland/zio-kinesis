package nl.vroste.zio.kinesis.client.zionative.leasecoordinator

import software.amazon.awssdk.services.dynamodb.model._

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

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
      case null       => builder.nul(true)
      case v: String  => builder.s(v)
      case v: Long    => builder.n(v.toString)
      case v: List[_] => builder.ss(v.map(_.toString).asJavaCollection)
      case v: Seq[_]  => builder.ss(v.map(_.toString).asJavaCollection)
      case v          =>
        throw new Exception(s"Could not convert value ${v} to attribute!")
    }
    builder.build()
  }

  def keySchemaElement(name: String, keyType: KeyType)                 =
    KeySchemaElement.builder().attributeName(name).keyType(keyType).build()
  def attributeDefinition(name: String, attrType: ScalarAttributeType) =
    AttributeDefinition.builder().attributeName(name).attributeType(attrType).build()

}
