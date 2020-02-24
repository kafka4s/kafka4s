package io.kafka4s.middlewares.dlq

import java.io.{PrintWriter, StringWriter}

import cats.implicits._
import cats.{Monad, MonadError, Semigroup}
import io.kafka4s.common.{Header, Record}
import io.kafka4s.producer.ProducerRecord

trait DeadLetter[F[_]] {
  def build(record: Record[F], throwable: Throwable): F[Record[F]]
}

object DeadLetter {

  def prefixed(prefix: String): String => String = root => s"$root$prefix"

  def apply[F[_]: MonadError[*[_], Throwable]](prefix: String): DeadLetter[F] =
    DeadLetter[F](prefixed(prefix))

  def apply[F[_]: MonadError[*[_], Throwable]](fn: String => String): DeadLetter[F] =
    (record, ex) =>
      (
        Header.of[F]("X-Exception-Message" -> getMessage(ex)),
        Header.of[F]("X-Stack-Trace"       -> getStackTrace(ex))
      ).mapN {
        case (message, stackTrace) =>
          ProducerRecord[F](record).put(message, stackTrace).copy(topic = fn(record.topic))
    }

  /** Gets a short message summarising the exception in the form
    * {ClassNameWithoutPackage}: {ThrowableMessage}
    *
    * Extracted from org.apache.commons.lang3.exception.ExceptionUtils.getMessage
    */
  def getMessage(throwable: Throwable): String = Option(throwable).fold("") { e =>
    s"${e.getClass.getSimpleName}: ${e.getMessage}"
  }

  /** Throw a checked exception without adding the exception to the throws
    * clause of the calling method.
    *
    * Extracted from org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace
    */
  def getStackTrace(throwable: Throwable): String = {
    val sw = new StringWriter
    val pw = new PrintWriter(sw, true)
    throwable.printStackTrace(pw)
    sw.getBuffer.toString
  }

  implicit def semigroup[F[_]: Monad] = new Semigroup[DeadLetter[F]] {
    override def combine(x: DeadLetter[F], y: DeadLetter[F]): DeadLetter[F] =
      (record, ex) => x.build(record, ex).flatMap(y.build(_, ex))
  }
}
