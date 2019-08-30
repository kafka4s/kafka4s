package io.kafka4s.consumer

import cats.ApplicativeError

private[kafka4s] class Implicits {
  implicit class ConsumerOps[F[_]](val consumer: Consumer[F]) {
    def orNotFound(implicit F: ApplicativeError[F, Throwable]): RecordConsumer[F] = Consumer.orNotFound(consumer)
  }
}
