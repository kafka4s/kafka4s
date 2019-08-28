package io.kafka4s.dsl

import cats.ApplicativeError
import io.kafka4s.{Consumer, RecordConsumer}

private[kafka4s] trait DslImplicits { self =>
  implicit class ConsumerOps[F[_]](val consumer: Consumer[F]) {
    def orNotFound(implicit F: ApplicativeError[F, Throwable]): RecordConsumer[F] = Consumer.orNotFound(consumer)
  }
}
