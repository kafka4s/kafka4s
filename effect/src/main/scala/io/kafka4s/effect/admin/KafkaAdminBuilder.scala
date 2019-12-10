package io.kafka4s.effect.admin

import cats.effect.Resource
import io.kafka4s.effect.consumer.KafkaConsumer

case class KafkaAdminBuilder[F[_]]() {
  def resource: Resource[F, KafkaConsumer[F]] = ???
}

object KafkaAdminBuilder {
//  def apply[F[_]: Sync](): KafkaAdminBuilder[F] = KafkaAdminBuilder[F]()
}
