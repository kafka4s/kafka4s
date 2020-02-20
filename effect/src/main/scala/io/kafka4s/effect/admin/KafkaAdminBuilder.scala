package io.kafka4s.effect.admin

import java.util.Properties

import cats.effect.{Resource, Sync}

case class KafkaAdminBuilder[F[_]] private (properties: Option[Properties]) {

  def resource(implicit F: Sync[F]): Resource[F, AdminEffect[F]] = for {
    admin <- properties.fold(AdminEffect.resource)(AdminEffect.resource(_))
  } yield admin
}

object KafkaAdminBuilder {

  def apply[F[_]: Sync]: KafkaAdminBuilder[F] =
    KafkaAdminBuilder[F](properties = None)
}
