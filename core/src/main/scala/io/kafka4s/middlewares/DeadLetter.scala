package io.kafka4s.middlewares

import java.io.{PrintWriter, StringWriter}

import io.kafka4s.consumer.Return.Err

//private class DeadLetter[F[_]](producer: Producer[F],
//                               name: Err[F] => String = DeadLetter.defaultName)
//                              (implicit F: ApplicativeError[F, Throwable]) {
//
//  /**
//   * Create a producer record from a consumer record enriched with the exception message and stack trace on the headers.
//   */
//  def buildDeadLetter(record: ConsumerRecord[F], ex: Throwable): ProducerRecord[F] = ???
//    // Add the exception information to the message header
//    val headers: Iterable[Header] = Record.headers(
//      Map(
//        DeadLetterMiddleware.EXCEPTION_HEADER   -> DeadLetterMiddleware.getMessage(ex),
//        DeadLetterMiddleware.STACK_TRACE_HEADER -> DeadLetterMiddleware.getStackTrace(ex)
//      )
//    )
//
//  private def handleError(record: ConsumerRecord[F], exception: Throwable): F[Unit] = ???
//    for {
//      _ <- F.delay(buildDeadLetter(r, exception)) >>= producer.send1.apply
//    } yield ()
//
//  def combine(consumer: Consumer[F]): Consumer[F] = Kleisli(record => OptionT(F.recoverWith(consumer.app(record).value) {
//    case e: Throwable => handleError(record, e).map(_.some)
//  }))
//}

object DeadLetter {
  def defaultName[F[_]]: Err[F] => String = (err: Err[F]) => s"${err.record.topic}-dlq"

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
