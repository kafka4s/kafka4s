package io.kafka4s.serdes

trait Deserializer[T] {
  def deserialize(value: Array[Byte]): Result[T]
}

object Deserializer {
  def apply[T](implicit D: Deserializer[T]): Deserializer[T] = D

  def apply[T](implicit S: Serde[T]): Deserializer[T] = new Deserializer[T] {
    def deserialize(value: Array[Byte]): Result[T] = S.deserialize(value)
  }
}
