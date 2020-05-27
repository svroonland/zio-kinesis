package nl.vroste.zio.kinesis.client.serde

import java.nio.ByteBuffer

import zio.RIO

/**
 * Serializer from values of some type T to a byte array
 *
 * @tparam R Environment available to the serializer
 * @tparam T
 */
trait Serializer[-R, -T] {
  def serialize(value: T): RIO[R, ByteBuffer]

  /**
   * Create a serializer for a type U based on the serializer for type T and a mapping function
   */
  def contramap[U](f: U => T): Serializer[R, U] =
    Serializer(u => serialize(f(u)))

  /**
   * Create a serializer for a type U based on the serializer for type T and an effectful mapping function
   */
  def contramapM[R1 <: R, U](f: U => RIO[R1, T]): Serializer[R1, U] =
    Serializer(u => f(u).flatMap(serialize))
}

object Serializer extends Serdes {

  /**
   * Create a serializer from a function
   */
  def apply[R, T](ser: T => RIO[R, ByteBuffer]): Serializer[R, T] =
    new Serializer[R, T] {
      override def serialize(value: T): RIO[R, ByteBuffer] =
        ser(value)
    }
}
