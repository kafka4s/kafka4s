package io.kafka4s.producer

import cats.implicits._
import cats.{ApplicativeError, Monad, Show}
import io.kafka4s.serdes.Serializer
import io.kafka4s.{Header, Headers, Record}

import scala.util.hashing.MurmurHash3

final case class ProducerRecord[F[_]](topic: String,
                                      key: Array[Byte]           = Array.emptyByteArray,
                                      value: Array[Byte]         = Array.emptyByteArray,
                                      headers: Headers[F]        = Headers.empty[F],
                                      partition: Option[Int]     = None) extends Record[F] {

  def put(header: Header[F]*): ProducerRecord[F] = this.copy(headers = headers ++ Headers[F](header.toList))

  override def toString: String = s"ProducerRecord(${this.show})"

  override def hashCode(): Int =
    MurmurHash3.bytesHash(key ++ value)
}

object ProducerRecord {
  def apply[F[_]](record: DefaultProducerRecord): ProducerRecord[F] =
    new ProducerRecord[F](
      topic      = record.topic(),
      key        = record.key(),
      value     = record.value(),
      headers   = Headers[F](record.headers()),
      partition = if (record.partition() == null) None else Some(record.partition())
    )

  def of[F[_]]: RecordPartiallyApplied[F] = new RecordPartiallyApplied[F]

  private[kafka4s] final class RecordPartiallyApplied[F[_]](val dummy: Boolean = false) extends AnyVal {
    def apply[T : Serializer](message: (String, T))(implicit F: ApplicativeError[F, Throwable]): F[ProducerRecord[F]] = {
      apply(message._1, message._2)
    }

    def apply[T](topic: String, value: T)(implicit F: ApplicativeError[F, Throwable],
                                          S: Serializer[T]): F[ProducerRecord[F]] = for {
      v <- F.fromEither(S.serialize(value))
    } yield ProducerRecord[F](
      topic,
      key       = Array.emptyByteArray,
      value     = v,
      headers   = Headers.empty[F],
      partition = None,
    )

    def apply[K, V](topic: String, key: K, value: V)(implicit F: Monad[F] with ApplicativeError[F, Throwable],
                                                     K: Serializer[K],
                                                     V: Serializer[V]): F[ProducerRecord[F]] = for {
      k <- F.fromEither(K.serialize(key))
      v <- F.fromEither(V.serialize(value))
    } yield ProducerRecord[F](
      topic,
      key       = k,
      value     = v,
      headers   = Headers.empty[F],
      partition = None,
    )

    def apply[K, V](topic: String, key: K, value: V, partition: Int)(implicit F: Monad[F] with ApplicativeError[F, Throwable],
                                                                     K: Serializer[K],
                                                                     V: Serializer[V]): F[ProducerRecord[F]] = for {
      k <- F.fromEither(K.serialize(key))
      v <- F.fromEither(V.serialize(value))
    } yield ProducerRecord[F](
      topic,
      key       = k,
      value     = v,
      headers   = Headers.empty[F],
      partition = Some(partition),
    )
  }

  implicit def show[F[_]](implicit S: Show[Record[F]]): Show[ProducerRecord[F]] = (record: ProducerRecord[F]) =>
    S.show(record)
}
