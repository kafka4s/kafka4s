package io.kafka4s.middlewares

import java.io.{PrintWriter, StringWriter}

import cats.data.{Kleisli, OptionT}
import cats.implicits._
import cats.{ApplicativeError, MonadError}
import io.kafka4s.common.{Header, Record}
import io.kafka4s.consumer.Return
import io.kafka4s.middlewares.DeadLetterQueue.DeadLetterBuilder
import io.kafka4s.producer.ProducerRecord
import io.kafka4s.{Consumer, Producer}

import scala.util.control.NonFatal

class DeadLetterQueue[F[_]] private (producer: Producer[F], fa: DeadLetterBuilder[F])(
  implicit F: MonadError[F, Throwable]) {

  def apply(consumer: Consumer[F]): Consumer[F] = Kleisli { record =>
    OptionT(F.recoverWith(consumer.apply(record).value) {
      case NonFatal(ex) =>
        for {
          r <- fa(Return.Err(record, ex))
          _ <- producer.send1(r)
        } yield Some(())
    })
  }
}

object DeadLetterQueue {

  type DeadLetterBuilder[F[_]] = Kleisli[F, Return.Err[F], ProducerRecord[F]]

  def apply[F[_]](producer: Producer[F], prefix: String = "dlq")(consumer: Consumer[F])(
    implicit F: MonadError[F, Throwable]): DeadLetterQueue[F] =
    new DeadLetterQueue(producer, DeadLetterQueue.defaultBuilder(record => s"${record.topic}-$prefix"))

  def apply[F[_]](producer: Producer[F], fn: Kleisli[F, ProducerRecord[F], ProducerRecord[F]])(consumer: Consumer[F])(
    implicit F: MonadError[F, Throwable]): DeadLetterQueue[F] =
    new DeadLetterQueue(producer, DeadLetterQueue.defaultBuilder[F](_.topic).andThen(fn.run))

  def defaultBuilder[F[_]](topicName: Record[F] => String)(
    implicit F: ApplicativeError[F, Throwable]): DeadLetterBuilder[F] = Kleisli {
    case Return.Err(record, ex) =>
      (
        Header.of[F]("X-Exception-Message" -> getMessage(ex)),
        Header.of[F]("X-Stack-Trace"       -> getStackTrace(ex))
      ).mapN {
        case (message, stackTrace) =>
          ProducerRecord[F](record).put(message, stackTrace).copy(topic = topicName(record))
      }
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
}
