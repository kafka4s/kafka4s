package io.kafka4s.producer

import cats.MonadError
import cats.data.Kleisli
import cats.implicits._
import io.kafka4s.common.Record
import io.kafka4s.consumer.ConsumerRecord
import io.kafka4s.serdes.Serializer

trait Producer[F[_]] {

  /**
    * The side-effect of sending a producer record
    */
  def send1: Kleisli[F, ProducerRecord[F], Return[F]]

  final def send[V](message: (String, V))(implicit F: MonadError[F, Throwable], SV: Serializer[V]): F[Return[F]] = {
    val (topic, value) = message
    send(topic, value)
  }

  final def send[V](topic: String, value: V)(implicit F: MonadError[F, Throwable], SV: Serializer[V]): F[Return[F]] =
    for {
      p <- ProducerRecord.of[F](topic, value)
      r <- send1(p)
    } yield r

  final def send[K, V](topic: String, key: K, value: V)(implicit F: MonadError[F, Throwable],
                                                        SK: Serializer[K],
                                                        SV: Serializer[V]): F[Return[F]] =
    for {
      p <- ProducerRecord.of[F](topic, key, value)
      r <- send1(p)
    } yield r

  final def send[K, V](topic: String, partition: Int, key: K, value: V)(implicit F: MonadError[F, Throwable],
                                                                        SK: Serializer[K],
                                                                        SV: Serializer[V]): F[Return[F]] =
    for {
      p <- ProducerRecord.of[F](topic, key, value, partition = partition)
      r <- send1(p)
    } yield r

  final def send(record: ConsumerRecord[F]): F[Return[F]] = {
    send1(
      ProducerRecord[F](
        topic      = record.topic,
        keyBytes   = record.keyBytes,
        valueBytes = record.valueBytes,
        headers    = record.headers,
        partition  = Some(record.partition)
      )
    )
  }

  final def send(record: ProducerRecord[F]): F[Return[F]] =
    send1(record)

  final def send(record: Record[F]): F[Return[F]] =
    send1(ProducerRecord[F](record))
}
