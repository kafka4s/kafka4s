package io.kafka4s

import io.kafka4s.serdes.Deserializer

trait Record[F[_]] {
  def topic: String

  def as[T](implicit D: Deserializer[T]): F[T]

  def key[T](implicit D: Deserializer[T]): F[T]

  def header[T](implicit D: Deserializer[T]): F[Option[T]]

  def headers: Map[String, Array[Byte]]

  def size: Long
}

