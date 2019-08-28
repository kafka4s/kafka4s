package io.kafka4s

import cats.ApplicativeError
import cats.implicits._
import io.kafka4s.serdes.{Deserializer, Serializer}
import org.apache.kafka.common.header.{Header => ApacheKafkaHeader}

final case class Header[F[_]](key: String, value: Array[Byte]) {
  def as[V](implicit F: ApplicativeError[F, Throwable], D: Deserializer[V]): F[V] =
    F.fromEither(D.deserialize(value))
  
  def size: Int = value.length
}

object Header {
  def apply[F[_]](header: ApacheKafkaHeader): Header[F] =
    Header(header.key(), header.value())

  private[kafka4s] final class HeaderPartiallyApplied[F[_]](val dummy: Boolean = false) extends AnyVal {
    def apply[V](keyValue: (String, V))(implicit F: ApplicativeError[F, Throwable], S: Serializer[V]): F[Header[F]] =
      apply(keyValue._1, keyValue._2)

    def apply[V](key: String, value: V)(implicit F: ApplicativeError[F, Throwable], S: Serializer[V]): F[Header[F]] =
      for {
        bytes <- F.fromEither(S.serialize(value))
      } yield Header(key, bytes)
  }

  def of[F[_]] = new HeaderPartiallyApplied[F]
}
