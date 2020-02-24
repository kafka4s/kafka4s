package io.kafka4s.serdes

import cats.Alternative
import cats.implicits._

trait Deserializer[A] {
  def deserialize(value: Array[Byte]): Result[A]
}

object Deserializer {
  def apply[A](implicit D: Deserializer[A]): Deserializer[A] = D

  def apply[A](implicit S: Serde[A]): Deserializer[A] = new Deserializer[A] {
    def deserialize(value: Array[Byte]): Result[A] = S.deserialize(value)
  }

  def from[A](f: Array[Byte] => Either[Throwable, A]): Deserializer[A] =
    new Deserializer[A] {
      def deserialize(in: Array[Byte]): Result[A] = f(in)
    }

  implicit val alternative = new Alternative[Deserializer] {
    def pure[A](a: A): Deserializer[A] = Deserializer.from(Function.const(Right(a)))

    def empty[A]: Deserializer[A] = Deserializer.from(Function.const(Left(new Error("No empty"))))

    def combineK[A](l: Deserializer[A], r: Deserializer[A]): Deserializer[A] =
      new Deserializer[A] {
        def deserialize(in: Array[Byte]): Result[A] = l.deserialize(in) orElse r.deserialize(in)
      }

    def ap[A, B](ff: Deserializer[A => B])(fa: Deserializer[A]): Deserializer[B] =
      new Deserializer[B] {
        def deserialize(in: Array[Byte]): Result[B] = fa.deserialize(in) ap ff.deserialize(in)
      }
  }
}
