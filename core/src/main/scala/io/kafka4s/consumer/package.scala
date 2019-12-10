package io.kafka4s

import cats.data.{Kleisli, NonEmptyList, OptionT}
import org.apache.kafka.clients.consumer.{Consumer => ApacheConsumer, ConsumerRecord => ApacheConsumerRecord}

package object consumer extends Implicits {
  private[kafka4s] type DefaultConsumer       = ApacheConsumer[Array[Byte], Array[Byte]]
  private[kafka4s] type DefaultConsumerRecord = ApacheConsumerRecord[Array[Byte], Array[Byte]]

  type Consumer[F[_]]      = Kleisli[OptionT[F, ?], ConsumerRecord[F], Unit]
  type BatchConsumer[F[_]] = Kleisli[OptionT[F, ?], NonEmptyList[ConsumerRecord[F]], Unit]

  type RecordConsumer[F[_]]      = Kleisli[F, ConsumerRecord[F], Return[F]]
  type BatchRecordConsumer[F[_]] = Kleisli[F, NonEmptyList[ConsumerRecord[F]], BatchReturn[F]]
}
