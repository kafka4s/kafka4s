package io.kafka4s.serdes

trait Serializer[T] {
  def serialize(value: T): Result[Array[Byte]]
}

object Serializer {
  def apply[T](implicit S: Serializer[T]): Serializer[T] = S

  def apply[T](implicit S: Serde[T]): Serializer[T] = new Serializer[T] {
    def serialize(value: T): Result[Array[Byte]] = S.serialize(value)
  }
}
