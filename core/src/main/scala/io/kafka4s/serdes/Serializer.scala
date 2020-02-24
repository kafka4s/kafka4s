package io.kafka4s.serdes

import cats.Alternative
import cats.implicits._

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

//  implicit val alternative = new Alternative[Serializer] {
//    def pure[A](a: A): Serializer[A] = Serializer.from(Function.const(Right(a)))
//
//    def empty[A]: Serializer[A] = Serializer.from(Function.const(Right(Array.emptyByteArray)))
//
//    def combineK[A](l: Serializer[A], r: Serializer[A]): Serializer[A] =
//      new Serializer[A] {
//        def serialize(in: A): Result[Array[Byte]] = l.serialize(in) orElse r.serialize(in)
//      }
//
//    def ap[A, B](ff: Serializer[A => B])(fa: Serializer[A]): Serializer[B] =
//      new Serializer[B] {
//        def serialize(in: A): Result[Array[Byte]] = fa.serialize(in) ap ff.serialize(in)
//      }
//  }
}
