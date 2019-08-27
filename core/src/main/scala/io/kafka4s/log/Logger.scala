package io.kafka4s.log

trait Logger[F[_]] {
  def log(message: Message): F[Unit]

  def trace(message: String): F[Unit] = log(Level.Trace(message, None))
  def debug(message: String): F[Unit] = log(Level.Debug(message, None))
  def info(message: String): F[Unit]  = log(Level.Info(message, None))
  def warn(message: String): F[Unit]  = log(Level.Warn(message, None))
  def error(message: String): F[Unit] = log(Level.Error(message, None))

  def trace(message: String, ex: Throwable): F[Unit] = log(Level.Trace(message, Some(ex)))
  def debug(message: String, ex: Throwable): F[Unit] = log(Level.Debug(message, Some(ex)))
  def info(message: String, ex: Throwable): F[Unit]  = log(Level.Info(message, Some(ex)))
  def warn(message: String, ex: Throwable): F[Unit]  = log(Level.Warn(message, Some(ex)))
  def error(message: String, ex: Throwable): F[Unit] = log(Level.Error(message, Some(ex)))
}
