package io.kafka4s.effect.log

trait Logger[F[_]] {
  def log(message: Message): F[Unit]
}
