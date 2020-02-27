package io.kafka4s.consumer

import cats.Show
import cats.data.NonEmptyList

sealed trait BatchReturn[F[_]] {
  def records: NonEmptyList[ConsumerRecord[F]]
}

object BatchReturn {
  final case class Ack[F[_]](records: NonEmptyList[ConsumerRecord[F]]) extends BatchReturn[F]
  final case class Err[F[_]](records: NonEmptyList[ConsumerRecord[F]], ex: Throwable) extends BatchReturn[F]

  implicit def show[F[_]](implicit S: Show[ConsumerRecord[F]]): Show[BatchReturn[F]] =
    (result: BatchReturn[F]) => result.records.map(S.show).toList.mkString(", ")
}
