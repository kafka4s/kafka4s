package io.kafka4s.effect.producer

import java.time.Instant

import cats.data.Kleisli
import cats.effect.{Concurrent, Resource, Sync}
import cats.implicits._
import io.kafka4s.common.ToKafka
import io.kafka4s.producer.{DefaultProducerRecord, Producer, ProducerRecord, Return}
import org.apache.kafka.clients.producer.ProducerConfig

class KafkaProducer[F[_]](config: KafkaProducerConfiguration, producer: ProducerEffect[F])(implicit F: Sync[F])
    extends Producer[F] {

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

object KafkaProducer {

  def resource[F[_]](builder: KafkaProducerBuilder[F])(implicit F: Concurrent[F]): Resource[F, KafkaProducer[F]] =
    for {
      config <- Resource.liftF(F.fromEither {
        if (builder.properties.isEmpty) KafkaProducerConfiguration.load
        else KafkaProducerConfiguration.loadFrom(builder.properties)
      })
      properties <- Resource.liftF(F.delay {
        val p = config.properties
        p.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
        p.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all")
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
        p
      })
      producer <- Resource.make(ProducerEffect[F](properties))(_.close)
    } yield new KafkaProducer[F](config, producer)
}
