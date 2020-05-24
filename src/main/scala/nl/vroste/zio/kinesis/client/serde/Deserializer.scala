package nl.vroste.zio.kinesis.client.serde

import java.nio.ByteBuffer

import zio.RIO

import scala.util.{ Failure, Success, Try }

/**
 * Deserializer from byte array to a value of some type T
 *
 * @tparam R Environment available to the deserializer
 * @tparam T Value type
 */
trait Deserializer[-R, +T] {
  def deserialize(data: ByteBuffer): RIO[R, T]

  /**
   * Create a deserializer for a type U based on the deserializer for type T and a mapping function
   */
  def map[U](f: T => U): Deserializer[R, U] = Deserializer(deserialize(_).map(f))

  /**
   * Create a deserializer for a type U based on the deserializer for type T and an effectful mapping function
   */
  def mapM[R1 <: R, U](f: T => RIO[R1, U]): Deserializer[R1, U] = Deserializer(deserialize(_).flatMap(f))

  /**
   * When this serializer fails, attempt to deserialize with the alternative
   *
   * If both deserializers fail, the error will be the last deserializer's exception.
   */
  def orElse[R1 <: R, U >: T](alternative: Deserializer[R1, U]): Deserializer[R1, U] =
    Deserializer(data => deserialize(data) orElse alternative.deserialize(data))

  /**
   * Serde that handles deserialization failures by returning a Task
   *
   * This is useful for explicitly handling deserialization failures.
   */
  def asTry: Deserializer[R, Try[T]] =
    Deserializer(deserialize(_).fold(e => Failure(e), v => Success(v)))
}

object Deserializer extends Serdes {

  /**
   * Create a deserializer from a function
   */
  def apply[R, T](deser: ByteBuffer => RIO[R, T]): Deserializer[R, T] =
    new Deserializer[R, T] {
      override def deserialize(data: ByteBuffer): RIO[R, T] =
        deser(data)
    }
}
