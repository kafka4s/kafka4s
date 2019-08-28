package io.kafka4s

import org.apache.kafka.common.header.{Headers => ApacheKafkaHeaders}

import scala.collection.JavaConverters._

final case class Headers[F[_]](headers: Seq[Header[F]]) extends AnyVal {
  def add(header: Header[F]*): Headers[F] = this.copy(headers = headers ++ header)

  def size: Int = headers.foldLeft(0)(_ + _.size)

  def find(key: String): Option[Header[F]] = headers.find(_.key == key)
}

object Headers {
  def empty[F[_]]: Headers[F] = Headers[F](Seq.empty)

  def apply[F[_]](headers: ApacheKafkaHeaders): Headers[F] =
    new Headers(headers.iterator().asScala.map(Header[F]).toSeq)
}
