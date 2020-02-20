package io.kafka4s.effect.producer

import cats.effect.{Concurrent, Resource}
import io.kafka4s.effect.config.ProducerConfiguration

case class KafkaProducerBuilder[F[_]]() {

  def resource(implicit F: Concurrent[F]): Resource[F, KafkaProducer[F]] =
    for {
      config   <- Resource.liftF(F.fromEither(ProducerConfiguration.load))
      producer <- Resource.liftF(ProducerEffect[F](config.toProducer))
    } yield new KafkaProducer[F](producer)
}
