package io.kafka4s

import cats.Functor
import cats.arrow.Arrow
import cats.implicits._

trait ToKafka[A, B] {
  def transform(a: A): B
}

object ToKafka {

  class PartiallyAppliedCasting[B](val dummy: Boolean = false) extends AnyVal {
    def transform[A](a: A)(implicit casting: ToKafka[A, B]): B = casting.transform(a)
  }

  def apply[B] = new PartiallyAppliedCasting[B]

  def apply[A, B](implicit casting: ToKafka[A, B]): ToKafka[A, B] = casting

  // Works with Option, List and so on...
  implicit def functor[F[_], A, B](implicit toKafka: ToKafka[A, B], F: Functor[F]): ToKafka[F[A], F[B]] =
    _.map(toKafka.transform)

  implicit val arrow: Arrow[ToKafka] = new Arrow[ToKafka] {
    def lift[A, B](f: A => B): ToKafka[A, B] = a => f(a)

    def compose[A, B, C](fb: ToKafka[B, C], fa: ToKafka[A, B]): ToKafka[A, C] =
      a => fb.transform(fa.transform(a))

    def first[A, B, C](fa: ToKafka[A, B]): ToKafka[(A, C), (B, C)] = new ToKafka[(A, C), (B, C)] {
      def transform(ac: (A, C)): (B, C) = fa.transform(ac._1) -> ac._2
    }
  }
}
