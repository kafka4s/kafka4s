package io.kafka4s.effect.producer

import java.util.Properties

import cats.effect.{Concurrent, Resource}
import io.kafka4s.effect.config

case class KafkaProducerBuilder[F[_]](properties: Properties) {

  type Self = KafkaProducerBuilder[F]

  def withProperties(properties: Properties): Self =
    copy(properties = properties)

  def withProperties(properties: Map[String, String]): Self =
    copy(properties = config.mapToProperties(properties))

  def resource(implicit F: Concurrent[F]): Resource[F, KafkaProducer[F]] =
    KafkaProducer.resource[F](builder = this)
}

object KafkaProducerBuilder {
  def apply[F[_]]: KafkaProducerBuilder[F] = KafkaProducerBuilder(properties = new Properties())
}
