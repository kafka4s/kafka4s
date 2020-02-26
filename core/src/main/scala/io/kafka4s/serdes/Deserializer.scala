package io.kafka4s.serdes

import cats.implicits._
import cats.{Monad, SemigroupK}

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

  implicit val deserializerInstances = new Monad[Deserializer] with SemigroupK[Deserializer] {
    def pure[A](a: A): Deserializer[A] = Deserializer.from(Function.const(Right(a)))

    def flatMap[A, B](fa: Deserializer[A])(f: A => Deserializer[B]): Deserializer[B] = new Deserializer[B] {
      def deserialize(value: Array[Byte]): Result[B] = fa.deserialize(value).flatMap(f(_).deserialize(value))
    }

    def tailRecM[A, B](a: A)(f: A => Deserializer[Either[A, B]]): Deserializer[B] = new Deserializer[B] {

      def deserialize(value: Array[Byte]): Result[B] =
        f(a).deserialize(value).flatMap(identity).leftMap(a => new IllegalArgumentException(a.getClass.getName))
    }

    def combineK[A](l: Deserializer[A], r: Deserializer[A]): Deserializer[A] =
      new Deserializer[A] {
        def deserialize(value: Array[Byte]): Result[A] = l.deserialize(value) orElse r.deserialize(value)
      }
  }
}
