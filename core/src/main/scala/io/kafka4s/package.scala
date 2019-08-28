package io

import cats.data.{Kleisli, OptionT, NonEmptyList}
import org.apache.kafka.clients.consumer.{Consumer => ApacheConsumer, ConsumerRecord => ApacheConsumerRecord}
import org.apache.kafka.clients.producer.{Producer => ApacheProducer, ProducerRecord => ApacheProducerRecord}

package object kafka4s {
  private[kafka4s] type DefaultConsumer       = ApacheConsumer[Array[Byte], Array[Byte]]
  private[kafka4s] type DefaultProducer       = ApacheProducer[Array[Byte], Array[Byte]]
  private[kafka4s] type DefaultConsumerRecord = ApacheConsumerRecord[Array[Byte], Array[Byte]]
  private[kafka4s] type DefaultProducerRecord = ApacheProducerRecord[Array[Byte], Array[Byte]]

  type Consumer[F[_]]      = Kleisli[OptionT[F, ?], ConsumerRecord[F], Unit]
  type BatchConsumer[F[_]] = Kleisli[OptionT[F, ?], NonEmptyList[ConsumerRecord[F]], Unit]

  type RecordConsumer[F[_]]      = Kleisli[F, ConsumerRecord[F], Return[F]]
  type BatchRecordConsumer[F[_]] = Kleisli[F, Seq[ConsumerRecord[F]], BatchReturn[F]]

  type RecordProducer[F[_]] = Kleisli[F, ProducerRecord[F], ProducerRecord[F]]
}
