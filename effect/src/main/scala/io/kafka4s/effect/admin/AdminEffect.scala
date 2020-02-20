package io.kafka4s.effect.admin

import java.util.Properties

import cats.effect.{Resource, Sync}
import io.kafka4s.effect.config.AdminConfiguration
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}

class AdminEffect[F[_]] private (admin: AdminClient)(implicit F: Sync[F]) {
  def createTopics(newTopics: Seq[NewTopic]): F[Unit] = ???

  def deleteTopics(topics: Seq[String]): F[Unit] = ???
}

object AdminEffect {

  def resource[F[_]](implicit F: Sync[F]): Resource[F, AdminEffect[F]] = for {
    config <- Resource.liftF(F.fromEither(AdminConfiguration.fromConfig))
    admin  <- Resource.make(F.delay(AdminClient.create(config.properties)))(admin => F.delay(admin.close()))
  } yield new AdminEffect[F](admin)

  def resource[F[_]](properties: Properties)(implicit F: Sync[F]): Resource[F, AdminEffect[F]] = for {
    admin <- Resource.make(F.delay(AdminClient.create(properties)))(admin => F.delay(admin.close()))
  } yield new AdminEffect[F](admin)
}
