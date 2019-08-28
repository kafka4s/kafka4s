package io.kafka4s.log

trait Logger[F[_]] {
  def log(message: Message): F[Unit]
}
