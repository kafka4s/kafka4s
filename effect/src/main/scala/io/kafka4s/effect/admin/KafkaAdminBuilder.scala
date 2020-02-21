package io.kafka4s.effect.admin

import java.util.Properties

import cats.effect.{Resource, Sync}
import io.kafka4s.effect.config

case class KafkaAdminBuilder[F[_]] private (properties: Properties) {

  type Self = KafkaAdminBuilder[F]

  def withProperties(properties: Properties): Self =
    copy(properties = properties)

  def withProperties(properties: Map[String, String]): Self =
    copy(properties = config.mapToProperties(properties))

  def resource(implicit F: Sync[F]): Resource[F, AdminEffect[F]] =
    for {
      config <- Resource.liftF(F.fromEither {
        if (properties.isEmpty) KafkaAdminConfiguration.load else KafkaAdminConfiguration.loadFrom(properties)
      })
      admin <- Resource.make(AdminEffect[F](config.properties))(_.close())
    } yield admin
}

object KafkaAdminBuilder {

  def apply[F[_]: Sync]: KafkaAdminBuilder[F] =
    KafkaAdminBuilder[F](properties = new Properties())
}
