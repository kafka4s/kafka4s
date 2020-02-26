package io.kafka4s.serdes

import cats.implicits._
import cats.{Contravariant, SemigroupK}

trait Serializer[A] {
  def serialize(value: A): Result[Array[Byte]]
}

object Serializer {
  def apply[A](implicit S: Serializer[A]): Serializer[A] = S

  def apply[A](implicit S: Serde[A]): Serializer[A] = new Serializer[A] {
    def serialize(value: A): Result[Array[Byte]] = S.serialize(value)
  }

  def from[A](f: A => Either[Throwable, Array[Byte]]): Serializer[A] =
    new Serializer[A] {
      def serialize(in: A): Result[Array[Byte]] = f(in)
    }

  implicit val serializerInstances = new Contravariant[Serializer] with SemigroupK[Serializer] {

    def contramap[A, B](fa: Serializer[A])(f: B => A): Serializer[B] = new Serializer[B] {
      def serialize(value: B): Result[Array[Byte]] = fa.serialize(f(value))
    }

    def combineK[A](x: Serializer[A], y: Serializer[A]): Serializer[A] = new Serializer[A] {
      def serialize(value: A): Result[Array[Byte]] = x.serialize(value) orElse y.serialize(value)
    }
  }
}
