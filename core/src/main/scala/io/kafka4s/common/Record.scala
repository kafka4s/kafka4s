package io.kafka4s.common

import java.util.Base64

import cats.implicits._
import cats.{ApplicativeError, Contravariant, Show}
import io.kafka4s.serdes.Deserializer

trait Record[F[_]] {
  def topic: String

  def keyBytes: Array[Byte]

  def valueBytes: Array[Byte]

  def headers: Headers[F]

  def header[T](key: String)(implicit F: ApplicativeError[F, Throwable], D: Deserializer[T]): F[Option[T]] =
    headers.find(_.key == key).traverse(_.as[T])

  def as[T](implicit F: ApplicativeError[F, Throwable], D: Deserializer[T]): F[T] =
    F.fromEither(D.deserialize(valueBytes))

  def key[T](implicit F: ApplicativeError[F, Throwable], D: Deserializer[T]): F[T] =
    F.fromEither(D.deserialize(keyBytes))
}

object Record {

  private val b64 = Base64.getUrlEncoder.withoutPadding()

  implicit def show[F[_]]: Show[Record[F]] =
    record => s"[${record.topic}#${b64.encodeToString(record.keyBytes ++ record.valueBytes)}]"
}
