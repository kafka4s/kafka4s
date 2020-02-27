package io.kafka4s.producer

import java.time.Instant

import cats.Show

sealed trait Return[F[_]] {
  def record: ProducerRecord[F]
}

object Return {
  final case class Ack[F[_]](record: ProducerRecord[F],
                             partition: Int,
                             offset: Option[Long],
                             timestamp: Option[Instant])
      extends Return[F]

  final case class Err[F[_]](record: ProducerRecord[F], ex: Throwable) extends Return[F]

  implicit def ackShow[F[_]]: Show[Ack[F]] =
    (result: Ack[F]) => s"${result.record.topic}-${result.partition}${result.offset.map(n => s"@$n").getOrElse("")}"

  implicit def errShow[F[_]](implicit S: Show[ProducerRecord[F]]): Show[Err[F]] =
    (result: Err[F]) => S.show(result.record)
}
