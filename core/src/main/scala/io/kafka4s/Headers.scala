package io.kafka4s

import cats.Show
import org.apache.kafka.common.header.{Headers => ApacheKafkaHeaders}

import scala.collection.JavaConverters._

final case class Headers[F[_]](values: Seq[Header[F]]) extends AnyVal {
  def add(header: Header[F]*): Headers[F] = this.copy(values = values ++ header)

  def size: Int = values.foldLeft(0)(_ + _.size)

  def find(key: String): Option[Header[F]] = values.find(_.key == key)

  override def toString: String =
    s"Headers(${values.map(Show[Header[F]].show).mkString(", ")})"
}

object Headers {
  def empty[F[_]]: Headers[F] = Headers[F](Seq.empty)

  def apply[F[_]](headers: ApacheKafkaHeaders): Headers[F] =
    new Headers(headers.iterator().asScala.map(Header[F]).toSeq)
}
