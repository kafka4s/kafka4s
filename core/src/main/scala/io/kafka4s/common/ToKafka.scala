package io.kafka4s.common

trait ToKafka[A] {
  type Result

  def transform(a: A): Result
}

object ToKafka {

  def apply[A](implicit T: ToKafka[A]): ToKafka[A] = T
}
