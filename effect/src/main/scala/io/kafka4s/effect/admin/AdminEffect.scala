package io.kafka4s.effect.admin

import java.time.{Duration => JDuration}
import java.util.Properties

import cats.effect.Sync
import cats.implicits._
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}

import scala.concurrent.duration._

class AdminEffect[F[_]] private (admin: AdminClient)(implicit F: Sync[F]) {
  def createTopics(newTopics: Seq[NewTopic]): F[Unit] = F.unit

  def deleteTopics(topics: Seq[String]): F[Unit] = F.unit

  def close(timeout: FiniteDuration = 30.seconds): F[Unit] =
    F.delay(admin.close(JDuration.ofMillis(timeout.toMillis)))
}

object AdminEffect {

  def apply[F[_]](properties: Properties)(implicit F: Sync[F]): F[AdminEffect[F]] =
    for {
      admin <- F.delay(AdminClient.create(properties))
    } yield new AdminEffect[F](admin)
}
