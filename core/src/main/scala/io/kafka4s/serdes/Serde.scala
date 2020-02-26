package io.kafka4s.serdes

import cats.implicits._
import cats.kernel.Semigroup
import cats.{Invariant, SemigroupK}

trait Serde[A] extends Serializer[A] with Deserializer[A]

object Serde {
  def apply[A](implicit serde: Serde[A]): Serde[A] = serde

  def apply[A](implicit S: Serializer[A], D: Deserializer[A]): Serde[A] = new Serde[A] {
    def deserialize(value: Array[Byte]): Result[A] = D.deserialize(value)

    def serialize(value: A): Result[Array[Byte]] = S.serialize(value)
  }

  implicit def serdeSemigroup[A] = new Semigroup[Serde[A]] {

    def combine(x: Serde[A], y: Serde[A]): Serde[A] = new Serde[A] {
      def deserialize(value: Array[Byte]): Result[A] = x.deserialize(value) orElse y.deserialize(value)

      def serialize(value: A): Result[Array[Byte]] = x.serialize(value) orElse y.serialize(value)
    }
  }

  implicit val serdeInstances = new Invariant[Serde] with SemigroupK[Serde] {

    def imap[A, B](fa: Serde[A])(f: A => B)(g: B => A): Serde[B] = new Serde[B] {
      def deserialize(value: Array[Byte]): Result[B] = fa.deserialize(value).map(f(_))

      def serialize(value: B): Result[Array[Byte]] = fa.serialize(g(value))
    }

    def combineK[A](x: Serde[A], y: Serde[A]): Serde[A] = new Serde[A] {
      def deserialize(value: Array[Byte]): Result[A] = x.deserialize(value) orElse y.deserialize(value)

      def serialize(value: A): Result[Array[Byte]] = x.serialize(value) orElse y.serialize(value)
    }
  }
}
