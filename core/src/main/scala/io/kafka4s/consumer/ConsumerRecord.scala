package io.kafka4s.consumer

import java.time.Instant

import cats.implicits._
import cats.{ApplicativeError, Monad, Show}
import io.kafka4s.common.{Headers, Record}
import io.kafka4s.serdes.Serializer

import scala.util.hashing.MurmurHash3

final case class ConsumerRecord[F[_]](topic: String,
                                      keyBytes: Array[Byte],
                                      valueBytes: Array[Byte],
                                      headers: Headers[F],
                                      offset: Long,
                                      partition: Int,
                                      timestamp: Instant)
    extends Record[F] {

  override def toString: String = s"ConsumerRecord(${this.show})"

  override def hashCode(): Int =
    MurmurHash3.bytesHash(keyBytes ++ valueBytes)
}

object ConsumerRecord {

  def apply[F[_]](record: DefaultConsumerRecord): ConsumerRecord[F] =
    new ConsumerRecord[F](
      topic      = record.topic(),
      keyBytes   = record.key(),
      valueBytes = record.value(),
      headers    = Headers[F](record.headers()),
      offset     = record.offset(),
      partition  = record.partition(),
      timestamp  = Instant.ofEpochMilli(record.timestamp())
    )

  def of[F[_]]: RecordPartiallyApplied[F] = new RecordPartiallyApplied[F]

  private[kafka4s] final class RecordPartiallyApplied[F[_]](val dummy: Boolean = false) extends AnyVal {

    def apply[T: Serializer](message: (String, T))(implicit F: ApplicativeError[F, Throwable]): F[ConsumerRecord[F]] = {
      apply(message._1, message._2)
    }

    def apply[T](topic: String, value: T)(implicit F: ApplicativeError[F, Throwable],
                                          S: Serializer[T]): F[ConsumerRecord[F]] =
      for {
        v <- F.fromEither(S.serialize(value))
      } yield ConsumerRecord[F](
        topic,
        keyBytes   = Array.emptyByteArray,
        valueBytes = v,
        headers    = Headers.empty[F],
        offset     = 0L,
        partition  = 0,
        timestamp  = Instant.now()
      )

    def apply[K, V](topic: String, key: K, value: V)(implicit F: Monad[F] with ApplicativeError[F, Throwable],
                                                     K: Serializer[K],
                                                     V: Serializer[V]): F[ConsumerRecord[F]] =
      for {
        k <- F.fromEither(K.serialize(key))
        v <- F.fromEither(V.serialize(value))
      } yield ConsumerRecord[F](
        topic,
        keyBytes   = k,
        valueBytes = v,
        headers    = Headers.empty[F],
        offset     = 0L,
        partition  = 0,
        timestamp  = Instant.now()
      )

    def apply[K, V](topic: String, key: K, value: V, partition: Int)(implicit F: Monad[F]
                                                                       with ApplicativeError[F, Throwable],
                                                                     K: Serializer[K],
                                                                     V: Serializer[V]): F[ConsumerRecord[F]] =
      for {
        k <- F.fromEither(K.serialize(key))
        v <- F.fromEither(V.serialize(value))
      } yield ConsumerRecord[F](
        topic,
        keyBytes   = k,
        valueBytes = v,
        headers    = Headers.empty[F],
        offset     = 0L,
        partition,
        timestamp = Instant.now()
      )

    def apply[K, V](topic: String, key: K, value: V, partition: Int, offset: Long)(
      implicit F: Monad[F] with ApplicativeError[F, Throwable],
      K: Serializer[K],
      V: Serializer[V]
    ): F[ConsumerRecord[F]] =
      for {
        k <- F.fromEither(K.serialize(key))
        v <- F.fromEither(V.serialize(value))
      } yield ConsumerRecord[F](
        topic,
        keyBytes   = k,
        valueBytes = v,
        headers    = Headers.empty[F],
        offset,
        partition,
        timestamp = Instant.now()
      )
  }

  implicit def show[F[_]]: Show[ConsumerRecord[F]] =
    (record: ConsumerRecord[F]) => s"[${record.topic}-${record.partition}@${record.offset}]"
}
