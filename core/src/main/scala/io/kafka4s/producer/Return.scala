package io.kafka4s.producer

import java.time.Instant

sealed trait Return[F[_]] {
  def record: ProducerRecord[F]
}

object Return {
  final case class Ack[F[_]](record: ProducerRecord[F], partition: Int, offset: Long, timestamp: Instant) extends Return[F]
  final case class Err[F[_]](record: ProducerRecord[F], ex: Throwable) extends Return[F]
}
