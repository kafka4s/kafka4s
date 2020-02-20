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
    for {
      config <- Resource.liftF(F.fromEither {
        if (properties.isEmpty) KafkaProducerConfiguration.load else KafkaProducerConfiguration.loadFrom(properties)
      })
      producer <- Resource.make(ProducerEffect[F](config.toProducer))(_.close)
    } yield new KafkaProducer[F](producer)
}

object KafkaProducerBuilder {
  def apply[F[_]]: KafkaProducerBuilder[F] = KafkaProducerBuilder(properties = new Properties())
}
