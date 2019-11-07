package io.kafka4s.serdes

trait Serde[T] extends Serializer[T] with Deserializer[T]

object Serde {
  def apply[T](implicit serde: Serde[T]): Serde[T] = serde

  def apply[T](implicit S: Serializer[T], D: Deserializer[T]): Serde[T] = new Serde[T] {
    def deserialize(value: Array[Byte]): Result[T] = D.deserialize(value)

    def serialize(value: T): Result[Array[Byte]] = S.serialize(value)
  }
}
