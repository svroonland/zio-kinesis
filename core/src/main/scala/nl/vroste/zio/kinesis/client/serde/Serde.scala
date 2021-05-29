package nl.vroste.zio.kinesis.client.serde

import java.nio.ByteBuffer

import zio.RIO

import scala.util.Try

/**
 * A serializer and deserializer for values of type T
 *
 * @tparam R Environment available to the deserializer
 * @tparam T Value type
 */
trait Serde[-R, T] extends Deserializer[R, T] with Serializer[R, T] {

  /**
   * Converts to a Serde of type U with pure transformations
   */
  def inmap[U](f: T => U)(g: U => T): Serde[R, U] =
    Serde(map(f))(contramap(g))

  /**
   * Convert to a Serde of type U with effectful transformations
   */
  def inmapM[R1 <: R, U](f: T => RIO[R1, U])(g: U => RIO[R1, T]): Serde[R1, U] =
    Serde(mapM(f))(contramapM(g))
}

object Serde extends Serdes {

  /**
   * Create a Serde from a deserializer and serializer function
   *
   * The (de)serializer functions can returned a failure ZIO with a Throwable to indicate (de)serialization failure
   */
  def apply[R, T](
    deser: ByteBuffer => RIO[R, T]
  )(ser: T => RIO[R, ByteBuffer]): Serde[R, T] =
    new Serde[R, T] {
      override def serialize(value: T): RIO[R, ByteBuffer]  =
        ser(value)
      override def deserialize(data: ByteBuffer): RIO[R, T] =
        deser(data)
    }

  /**
   * Create a Serde from a deserializer and serializer function
   */
  def apply[R, T](deser: Deserializer[R, T])(ser: Serializer[R, T]): Serde[R, T] =
    new Serde[R, T] {
      override def serialize(value: T): RIO[R, ByteBuffer]  =
        ser.serialize(value)
      override def deserialize(data: ByteBuffer): RIO[R, T] =
        deser.deserialize(data)
    }

  implicit def deserializerWithError[R, T](implicit deser: Deserializer[R, T]): Deserializer[R, Try[T]] =
    deser.asTry
}
