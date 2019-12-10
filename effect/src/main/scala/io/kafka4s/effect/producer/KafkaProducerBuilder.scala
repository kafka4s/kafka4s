package io.kafka4s.effect.producer

import cats.effect.Resource

case class KafkaProducerBuilder[F[_]]() {
  def resource: Resource[F, KafkaProducer[F]] = ???
}
