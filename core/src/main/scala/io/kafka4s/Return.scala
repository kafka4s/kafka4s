package io.kafka4s

import cats.Show

sealed trait Return[F[_]] {
  def record: ConsumerRecord[F]
}

object Return {
  final case class Ack[F[_]](record: ConsumerRecord[F]) extends Return[F]
  final case class Err[F[_]](record: ConsumerRecord[F], ex: Throwable) extends Return[F]

  implicit def show[F[_]](implicit S: Show[ConsumerRecord[F]]): Show[Return[F]] =
    (`return`: Return[F]) => S.show(`return`.record)
}
