package io.kafka4s.common

import cats.implicits._
import cats.{Eval, Foldable, Monoid, Show}
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.header.{Header => ApacheKafkaHeader, Headers => ApacheKafkaHeaders}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

final class Headers[F[_]] private (private val headers: List[Header[F]]) extends AnyVal {

  def toList: List[Header[F]] = headers

  def isEmpty: Boolean = headers.isEmpty

  def nonEmpty: Boolean = headers.nonEmpty

  def drop(n: Int): Headers[F] = if (n == 0) this else new Headers(headers.drop(n))

  def iterator: Iterator[Header[F]] = headers.iterator

  def size: Int = headers.foldLeft(0)(_ + _.size)

  def length: Int = headers.length

  def exists(f: Header[F] => Boolean): Boolean =
    headers.exists(f)

  def forall(f: Header[F] => Boolean): Boolean =
    headers.forall(f)

  def find(f: Header[F] => Boolean): Option[Header[F]] =
    headers.find(f)

  def count(f: Header[F] => Boolean): Int =
    headers.count(f)

  /** Attempt to get a [[Header]] from this collection of headers
    *
    * @param key name of the header to find
    * @return a scala.Option possibly containing the resulting [[Header]]
    */
  def get(key: String): Option[Header[F]] = headers.find(_.key == key)

  /** Make a new collection adding the specified headers, replacing existing headers of singleton type
    * The passed headers are assumed to contain no duplicate Singleton headers.
    *
    * @param in multiple [[Header]] to append to the new collection
    * @return a new [[Headers]] containing the sum of the initial and input headers
    */
  def put(in: Header[F]*): Headers[F] =
    if (in.isEmpty) this
    else if (this.isEmpty) new Headers[F](in.toList)
    else this ++ Headers[F](in.toList)

  /** Concatenate the two collections
    * If the resulting collection is of Headers type, duplicate Singleton headers will be removed from
    * this Headers collection.
    *
    * @param that collection to append
    */
  def ++(that: Headers[F]): Headers[F] =
    if (that.isEmpty) this
    else if (this.isEmpty) that
    else {
      val hs  = that.toList
      val acc = new ListBuffer[Header[F]]
      this.headers.foreach {
        case h if !hs.exists(_.key == h.key) => acc += h
        case _                               => // NOOP, drop non recurring header that already exists
      }

      val h = new Headers[F](acc.prependToList(hs))
      h
    }

  def map[A](f: Header[F] => A): Seq[A] = headers.map(f)

  def filterNot(f: Header[F] => Boolean): Headers[F] =
    Headers[F](headers.filterNot(f))

  def filter(f: Header[F] => Boolean): Headers[F] =
    Headers[F](headers.filter(f))

  def collectFirst[B](f: PartialFunction[Header[F], B]): Option[B] =
    headers.collectFirst(f)

  def foldMap[B: Monoid](f: Header[F] => B): B =
    headers.foldMap(f)

  def foldLeft[A](z: A)(f: (A, Header[F]) => A): A =
    headers.foldLeft(z)(f)

  def foldRight[A](z: Eval[A])(f: (Header[F], Eval[A]) => Eval[A]): Eval[A] =
    Foldable[List].foldRight(headers, z)(f)

  def foreach(f: Header[F] => Unit): Unit =
    headers.foreach(f)

  override def toString: String =
    s"Headers(${headers.map(Show[Header[F]].show).mkString(", ")})"
}

object Headers {
  def empty[F[_]]: Headers[F] = new Headers[F](List.empty)

  def apply[F[_]](headers: List[Header[F]]): Headers[F] = new Headers(headers)

  def apply[F[_]](headers: ApacheKafkaHeaders): Headers[F] =
    new Headers(headers.iterator().asScala.map(Header[F]).toList)

  implicit def toKafka[F[_]]: ToKafka[Headers[F]] = new ToKafka[Headers[F]] {
    type Result = ApacheKafkaHeaders

    def transform(headers: Headers[F]): ApacheKafkaHeaders = {
      val h = headers.map(h => ToKafka[Header[F]].transform(h))
      new RecordHeaders(h.asInstanceOf[Iterable[ApacheKafkaHeader]].asJava)
    }
  }
}
