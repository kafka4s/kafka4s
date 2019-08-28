package io.kafka4s

import cats.data.OptionT
import cats.{ApplicativeError, Monad, Show}
import io.kafka4s.serdes.Deserializer

import scala.util.hashing.MurmurHash3

trait Record[F[_]] {
  def topic: String

  def key: Array[Byte]

  def value: Array[Byte]

  def headers: Headers[F]

  def header[T](key: String)
               (implicit F: Monad[F] with ApplicativeError[F, Throwable], D: Deserializer[T]): F[Option[T]] = {
    for {
      h <- OptionT.fromOption[F](headers.find(key))
      t <- OptionT.liftF(F.fromEither(D.deserialize(h.value)))
    } yield t
  }.value

  def as[T](implicit F: ApplicativeError[F, Throwable], D: Deserializer[T]): F[T] =
    F.fromEither(D.deserialize(value))

  def keyAs[T](implicit F: ApplicativeError[F, Throwable], D: Deserializer[T]): F[T] =
    F.fromEither(D.deserialize(key))

  def size: Int = key.length + value.length + headers.size

  override def toString(): String = {
    s"[$topic%${MurmurHash3.bytesHash(key ++ value)}]"
  }
}

object Record { self =>
  implicit def show[F[_]]: Show[Record[F]] = _.toString()
}
