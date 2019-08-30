package io.kafka4s.consumer

sealed trait Return[F[_]] {
  def record: ConsumerRecord[F]
}

object Return {
  final case class Ack[F[_]](record: ConsumerRecord[F]) extends Return[F]
  final case class Err[F[_]](record: ConsumerRecord[F], ex: Throwable) extends Return[F]

  
}
