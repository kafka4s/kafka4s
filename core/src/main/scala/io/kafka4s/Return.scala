package io.kafka4s

sealed trait Return[F[_]] {
  def record: Record[F]
}

object Return {
  final case class Ack[F[_]](record: Record[F]) extends Return[F]
  final case class Err[F[_]](record: Record[F], ex: Exception) extends Return[F]
}
