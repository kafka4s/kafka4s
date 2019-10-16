package io.kafka4s

import java.util.Base64

import cats.data.OptionT
import cats.{ApplicativeError, Monad, Show}
import io.kafka4s.serdes.Deserializer

trait Record[F[_]] {
  def topic: String

  def key: Array[Byte]

  def value: Array[Byte]

  def headers: Headers[F]

  def header[T](key: String)
               (implicit F: Monad[F] with ApplicativeError[F, Throwable], D: Deserializer[T]): F[Option[T]] = {
    for {
      h <- OptionT.fromOption[F](headers.find(_.key == key))
      t <- OptionT.liftF(F.fromEither(D.deserialize(h.value)))
    } yield t
  }.value

  def as[T](implicit F: ApplicativeError[F, Throwable], D: Deserializer[T]): F[T] =
    F.fromEither(D.deserialize(value))

  def keyAs[T](implicit F: ApplicativeError[F, Throwable], D: Deserializer[T]): F[T] =
    F.fromEither(D.deserialize(key))

  def size: Int = key.length + value.length + headers.size
}

object Record { self =>

  private val b64 = Base64.getUrlEncoder.withoutPadding()

  implicit def show[F[_]]: Show[Record[F]] = (record) =>
    s"[${record.topic}#${b64.encodeToString(record.key ++ record.value)}]"
}
