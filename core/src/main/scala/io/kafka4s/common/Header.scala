package io.kafka4s.common

import java.util.Base64

import cats.implicits._
import cats.{ApplicativeError, Eq, Show}
import io.kafka4s.serdes.{Deserializer, Serializer}
import org.apache.kafka.common.header.{Header => ApacheKafkaHeader}

import scala.util.hashing.MurmurHash3

final case class Header[F[_]](key: String, value: Array[Byte]) {

  def as[V](implicit F: ApplicativeError[F, Throwable], D: Deserializer[V]): F[V] =
    F.fromEither(D.deserialize(value))

  val size: Int = key.getBytes.length + value.length

  override def toString: String = s"Header(${this.show})"

  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[this.type] && obj.hashCode() == this.hashCode()
  }

  override def hashCode(): Int =
    MurmurHash3.bytesHash(key.getBytes ++ value)
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

  private val b64 = Base64.getUrlEncoder.withoutPadding()

  implicit def show[F[_]]: Show[Header[F]] =
    header => s"${header.key} -> ${b64.encodeToString(header.value)}"

  implicit def eq[F[_]]: Eq[Header[F]] = (x, y) => x.hashCode() == y.hashCode()
}
