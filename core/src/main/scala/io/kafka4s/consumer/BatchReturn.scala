package io.kafka4s.consumer

import cats.data.NonEmptyList
import io.kafka4s.common.Record

sealed trait BatchReturn[F[_]] {
  def records: NonEmptyList[Record[F]]
}

object BatchReturn {
  final case class Ack[F[_]](records: NonEmptyList[Record[F]]) extends BatchReturn[F]
  final case class Err[F[_]](records: NonEmptyList[Record[F]], ex: Throwable) extends BatchReturn[F]
}
