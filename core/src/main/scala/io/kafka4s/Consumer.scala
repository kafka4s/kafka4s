package io.kafka4s

import cats.{ApplicativeError, Monad}
import cats.data.{Kleisli, OptionT}
import io.kafka4s.Return.{Ack, Err}

import scala.util.control.NonFatal

object Consumer {
  final case class TopicNotFound(topic: String) extends RuntimeException(s"Consumer not found for topic '$topic'")

  def of[F[_]: Monad](pf: PartialFunction[ConsumerRecord[F], F[Unit]]): Consumer[F] =
    Kleisli(record => pf.andThen(OptionT.liftF(_)).applyOrElse(record, Function.const(OptionT.none)))

  def empty[F[_]: Monad]: Consumer[F] =
    Consumer.of[F](PartialFunction.empty)

  private[kafka4s] def notFoundErr[F[_]](record: ConsumerRecord[F]): Return[F] = Err(record, TopicNotFound(record.topic))

  private[kafka4s] def orNotFound[F[_]](consumer: Consumer[F])(implicit F: ApplicativeError[F, Throwable]): RecordConsumer[F] =
    Kleisli(
      record =>
        F.recover(consumer(record).fold[Return[F]](notFoundErr(record))(_ => Ack(record))) {
          case NonFatal(ex) => Err(record, ex)
        }
    )
}
