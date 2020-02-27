package io.kafka4s.effect.log

import cats.{Applicative, Apply, Monoid}

trait Logger[F[_]] { self =>
  def log(message: Message): F[Unit]

  @inline def trace(message: String): F[Unit] = log(Level.Trace(message, None))
  @inline def debug(message: String): F[Unit] = log(Level.Debug(message, None))
  @inline def info(message: String): F[Unit]  = log(Level.Info(message, None))
  @inline def warn(message: String): F[Unit]  = log(Level.Warn(message, None))
  @inline def error(message: String): F[Unit] = log(Level.Error(message, None))

  @inline def trace(message: String, ex: Throwable): F[Unit] =
    log(Level.Trace(message, Some(ex)))

  @inline def debug(message: String, ex: Throwable): F[Unit] =
    log(Level.Debug(message, Some(ex)))

  @inline def info(message: String, ex: Throwable): F[Unit] =
    log(Level.Info(message, Some(ex)))

  @inline def warn(message: String, ex: Throwable): F[Unit] =
    log(Level.Warn(message, Some(ex)))

  @inline def error(message: String, ex: Throwable): F[Unit] =
    log(Level.Error(message, Some(ex)))

  def andThen(that: Logger[F])(implicit ap: Apply[F]): Logger[F] = new Logger[F] {

    def log(message: Message): F[Unit] =
      ap.productR(self.log(message))(that.log(message))
  }
}

object Logger {

  def void[F[_]](implicit F: Applicative[F]): Logger[F] = new Logger[F] {
    def log(message: Message): F[Unit] = F.unit
  }

  implicit def monoidInstanceForLogger[F[_]](implicit F: Applicative[F]): Monoid[Logger[F]] = new Monoid[Logger[F]] {
    def empty: Logger[F] = Logger.void

    def combine(x: Logger[F], y: Logger[F]): Logger[F] = x andThen y
  }
}
