package nl.vroste.zio.kinesis.client.zionative.leaserepository

import zio.aws.dynamodb.model._
import zio.aws.dynamodb.model.primitives.{
  AttributeName,
  KeySchemaAttributeName,
  NullAttributeValue,
  NumberAttributeValue,
  StringAttributeValue
}

object DynamoDbUtil {
  type DynamoDbItem = Map[AttributeName, AttributeValue]

  object DynamoDbItem {
    def apply(items: (String, AttributeValue)*): DynamoDbItem =
      Map(items.map { case (name, value) => AttributeName(name) -> value }: _*)
    val empty                                                 = apply()
  }

  def expectedAttributeValue[T](v: T): ExpectedAttributeValue =
    ExpectedAttributeValue(value = Some(attributeValue(v)))

  def putAttributeValueUpdate[T](value: T): AttributeValueUpdate =
    AttributeValueUpdate(action = Some(AttributeAction.PUT), value = Some(attributeValue(value)))

  def deleteAttributeValueUpdate: AttributeValueUpdate =
    AttributeValueUpdate(action = Some(AttributeAction.DELETE), value = None) // TODO does none work?

  object ImplicitConversions {
    implicit def toAttributeValue[T](value: T): AttributeValue =
      attributeValue[T](value)
  }

  def attributeValue[T](value: T): AttributeValue =
    value match {
      case null       => AttributeValue(nul = Some(NullAttributeValue(true)))
      case v: String  => AttributeValue(s = Some(StringAttributeValue(v)))
      case v: Long    => AttributeValue(n = Some(NumberAttributeValue(v.toString)))
      case v: List[_] => AttributeValue(ss = Some(v.map(_.toString).map(StringAttributeValue(_))))
      case v: Seq[_]  => AttributeValue(ss = Some(v.map(_.toString).map(StringAttributeValue(_)).toList))
      case v          =>
        throw new Exception(s"Could not convert value ${v} to attribute!")
    }

  def keySchemaElement(name: String, keyType: KeyType) =
    KeySchemaElement(KeySchemaAttributeName(name), keyType)

  def attributeDefinition(name: String, attrType: ScalarAttributeType) =
    AttributeDefinition(KeySchemaAttributeName(name), attrType)

}
