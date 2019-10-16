package io.kafka4s.effect.log

import cats.{Applicative, Apply, Monoid}

/**
 * Suspend log side effects allowing monadic composition in terms of an effect.
 */
trait Log[F[_]] { self =>
  protected def log(logger: Logger[F], message: Message): F[Unit]

  @inline def trace(message: String)(implicit logger: Logger[F]): F[Unit] = log(logger, Level.Trace(message, None))
  @inline def debug(message: String)(implicit logger: Logger[F]): F[Unit] = log(logger, Level.Debug(message, None))
  @inline def info(message: String)(implicit logger: Logger[F]): F[Unit]  = log(logger, Level.Info(message, None))
  @inline def warn(message: String)(implicit logger: Logger[F]): F[Unit]  = log(logger, Level.Warn(message, None))
  @inline def error(message: String)(implicit logger: Logger[F]): F[Unit] = log(logger, Level.Error(message, None))

  @inline def trace(message: String, ex: Throwable)(implicit logger: Logger[F]): F[Unit] = log(logger, Level.Trace(message, Some(ex)))
  @inline def debug(message: String, ex: Throwable)(implicit logger: Logger[F]): F[Unit] = log(logger, Level.Debug(message, Some(ex)))
  @inline def info(message: String, ex: Throwable)(implicit logger: Logger[F]): F[Unit]  = log(logger, Level.Info(message, Some(ex)))
  @inline def warn(message: String, ex: Throwable)(implicit logger: Logger[F]): F[Unit]  = log(logger, Level.Warn(message, Some(ex)))
  @inline def error(message: String, ex: Throwable)(implicit logger: Logger[F]): F[Unit] = log(logger, Level.Error(message, Some(ex)))

  def andThen(that: Log[F])(implicit ap: Apply[F]): Log[F] =
    (logger: Logger[F], message: Message) => ap.productR(self.log(logger, message))(that.log(logger, message))
}

object Log {
  implicit def apply[F[_]]: Log[F] = (logger: Logger[F], message: Message) => logger.log(message)

  def void[F[_]](implicit F: Applicative[F]): Log[F] = (_: Logger[F], _: Message) => F.unit

  implicit def monoidInstanceForLogger[F[_]](implicit F: Applicative[F]): Monoid[Log[F]] = new Monoid[Log[F]] {
    def empty: Log[F] = Log.void

    def combine(x: Log[F], y: Log[F]): Log[F] = x andThen y
  }
}
