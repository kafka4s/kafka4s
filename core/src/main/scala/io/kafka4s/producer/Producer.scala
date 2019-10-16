package io.kafka4s.producer

import cats.data.Kleisli
import cats.implicits._
import cats.{ApplicativeError, Monad}
import io.kafka4s.Header
import io.kafka4s.consumer.ConsumerRecord
import io.kafka4s.serdes.Serializer

import scala.reflect.runtime.universe.{TypeTag, typeOf}

trait Producer[F[_]] {
  def send1: Kleisli[F, ProducerRecord[F], Return[F]]
  
  def keyTypeHeaderName: String = "X-Key-ClassName"
  def valueTypeHeaderName: String = "X-Value-ClassName"

  def send[V](message: (String, V))(implicit F: Monad[F] with ApplicativeError[F, Throwable],
                                    SV: Serializer[V],
                                    TV: TypeTag[V]): F[Return[F]] = {
    val (topic, value) = message
    send(topic, value)
  }

  def send[V](topic: String, value: V)(implicit F: Monad[F] with ApplicativeError[F, Throwable],
                                       SV: Serializer[V],
                                       TV: TypeTag[V]): F[Return[F]] =
    for {
      vb <- F.fromEither(SV.serialize(value))
      tv <- Header.of[F](valueTypeHeaderName, typeOf[V].typeSymbol.fullName)
      p = ProducerRecord[F](topic, value = vb).put(tv)
      r <- send1(p)
    } yield r

  def send[K, V](topic: String, key: K, value: V)(implicit F: Monad[F] with ApplicativeError[F, Throwable],
                                                  SK: Serializer[K],
                                                  SV: Serializer[V],
                                                  TK: TypeTag[K],
                                                  TV: TypeTag[V]): F[Return[F]] =
    for {
      kb <- F.fromEither(SK.serialize(key))
      vb <- F.fromEither(SV.serialize(value))
      tk <- Header.of[F](keyTypeHeaderName, typeOf[K].typeSymbol.fullName)
      tv <- Header.of[F](valueTypeHeaderName, typeOf[V].typeSymbol.fullName)
      p = ProducerRecord[F](topic, key = kb, value = vb).put(tv, tk)
      r <- send1(p)
    } yield r

  def send[K, V](topic: String, partition: Int, key: K, value: V)(implicit F: Monad[F] with ApplicativeError[F, Throwable],
                                                                  SK: Serializer[K],
                                                                  SV: Serializer[V],
                                                                  TK: TypeTag[K],
                                                                  TV: TypeTag[V]): F[Return[F]] =
    for {
      kb <- F.fromEither(SK.serialize(key))
      vb <- F.fromEither(SV.serialize(value))
      tk <- Header.of[F](keyTypeHeaderName, typeOf[K].typeSymbol.fullName)
      tv <- Header.of[F](valueTypeHeaderName, typeOf[V].typeSymbol.fullName)
      p = ProducerRecord[F](topic, partition = Some(partition), key = kb, value = vb).put(tv, tk)
      r <- send1(p)
    } yield r

  def send(record: ConsumerRecord[F]): F[Return[F]] = {
    send1(ProducerRecord[F](
      topic     = record.topic,
      key       = record.key,
      value     = record.value,
      headers   = record.headers,
      partition = Some(record.partition),
    ))
  }
}
