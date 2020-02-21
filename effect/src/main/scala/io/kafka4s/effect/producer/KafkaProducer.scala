package io.kafka4s.effect.producer

import java.time.Instant

import cats.data.Kleisli
import cats.effect.Sync
import cats.implicits._
import io.kafka4s.Producer
import io.kafka4s.common.ToKafka
import io.kafka4s.producer.{DefaultProducerRecord, ProducerRecord, Return}

class KafkaProducer[F[_]](producer: ProducerEffect[F])(implicit F: Sync[F]) extends Producer[F] {

  def atomic[A](fa: F[A]): F[A] = producer.transaction(fa)

  /**
    * The side-effect of sending a producer record
    */
  def send1: Kleisli[F, ProducerRecord[F], Return[F]] = Kleisli { record =>
    for {
      producerRecord <- F.delay(ToKafka[ProducerRecord[F]].transform(record))
      metadata       <- producer.send(producerRecord.asInstanceOf[DefaultProducerRecord]).attempt
    } yield
      metadata match {
        case Right(m) =>
          Return.Ack(record,
                     m.partition(),
                     Option(m.offset()).filter(_ => m.hasOffset),
                     Option(m.timestamp()).filter(_ => m.hasTimestamp).map(Instant.ofEpochMilli))

        case Left(ex) =>
          Return.Err(record, ex)
      }
  }
}
