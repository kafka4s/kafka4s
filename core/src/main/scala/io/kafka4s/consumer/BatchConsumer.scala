package io.kafka4s.consumer

import cats.data.{Kleisli, NonEmptyList, OptionT}
import cats.{ApplicativeError, Monad}
import io.kafka4s.consumer.BatchReturn.{Ack, Err}

import scala.util.control.NonFatal

object BatchConsumer {
  def of[F[_]: Monad](pf: PartialFunction[NonEmptyList[ConsumerRecord[F]], F[Unit]]): BatchConsumer[F] =
    Kleisli(record => pf.andThen(OptionT.liftF(_)).applyOrElse(record, Function.const(OptionT.none)))

  def empty[F[_]: Monad]: BatchConsumer[F] =
    BatchConsumer.of[F](PartialFunction.empty)

  private[kafka4s] def notFoundErr[F[_]](records: NonEmptyList[ConsumerRecord[F]]): BatchReturn[F] = Err(records, TopicNotFound(records.head.topic))

  private[kafka4s] def orNotFound[F[_]](consumer: BatchConsumer[F])(implicit F: ApplicativeError[F, Throwable]): BatchRecordConsumer[F] =
    Kleisli(
      records =>
        F.recover(consumer(records).fold[BatchReturn[F]](notFoundErr(records))(_ => Ack(records))) {
          case NonFatal(ex) => Err(records, ex)
        }
    )
}
