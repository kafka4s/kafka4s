package io.kafka4s.effect.log

import cats.effect.Sync

trait Logger[F[_]] {
  def log(message: Message): F[Unit]
}

object Logger {
  implicit def apply[F[_]](implicit F: Sync[F]): Logger[F] = ???
}
