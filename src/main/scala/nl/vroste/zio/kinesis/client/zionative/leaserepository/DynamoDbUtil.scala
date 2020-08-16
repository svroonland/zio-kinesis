package nl.vroste.zio.kinesis.client.zionative.leaserepository

import io.github.vigoo.zioaws.dynamodb.model._

import scala.reflect.ClassTag

object DynamoDbUtil {
  type DynamoDbItem = Map[String, AttributeValue]

  object DynamoDbItem {
    def apply(items: (String, AttributeValue)*): DynamoDbItem = Map(items: _*)
    val empty                                                 = apply()
  }

  def expectedAttributeValue[T: ClassTag](v: T): ExpectedAttributeValue =
    ExpectedAttributeValue(value = Some(attributeValue(v)))

  def putAttributeValueUpdate[T: ClassTag](value: T): AttributeValueUpdate =
    AttributeValueUpdate(action = Some(AttributeAction.PUT), value = Some(attributeValue(value)))

  def deleteAttributeValueUpdate: AttributeValueUpdate =
    AttributeValueUpdate(action = Some(AttributeAction.DELETE), value = None) // TODO does none work?

  object ImplicitConversions {
    implicit def toAttributeValue[T: ClassTag](value: T): AttributeValue =
      attributeValue[T](value)
  }

  def attributeValue[T: ClassTag](value: T): AttributeValue =
    value match {
      case null       => AttributeValue(nul = Some(true))
      case v: String  => AttributeValue(s = Some(v))
      case v: Long    => AttributeValue(n = Some(v.toString))
      case v: List[_] => AttributeValue(ss = Some(v.map(_.toString)))
      case v: Seq[_]  => AttributeValue(ss = Some(v.map(_.toString).toList))
      case v          =>
        throw new Exception(s"Could not convert value ${v} to attribute!")
    }

  def keySchemaElement(name: String, keyType: KeyType) =
    KeySchemaElement(name, keyType)

  def attributeDefinition(name: String, attrType: ScalarAttributeType) =
    AttributeDefinition(name, attrType)

}
