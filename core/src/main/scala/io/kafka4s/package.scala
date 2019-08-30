package io

package object kafka4s {
  type Producer[F[_]]      = producer.Producer[F]
  type Consumer[F[_]]      = consumer.Consumer[F]
  type BatchConsumer[F[_]] = consumer.BatchConsumer[F]

  type RecordConsumer[F[_]]      = consumer.RecordConsumer[F]
  type BatchRecordConsumer[F[_]] = consumer.BatchRecordConsumer[F]
}
