package io.kafka4s.dsl

import cats.data.NonEmptyList
import io.kafka4s.common.{Header, Record}
import io.kafka4s.consumer.{ConsumerImplicits, ConsumerRecord}
import io.kafka4s.serdes.Deserializer

private[kafka4s] trait DslConsumer extends ConsumerImplicits {

  /**
    * Combine different matchers
    */
  object & {
    def unapply[A](a: A): Some[(A, A)] = Some((a, a))
  }

  /**
    * Pattern matching for Record topic in both batch and single
    */
  object Topic {
    def unapply[F[_]](record: Record[F]): Option[String] = Some(record.topic)
  }

  /**
    * Pattern matching for a batch of Records topic
    */
  object BatchTopic {
    def unapply[F[_]](records: NonEmptyList[Record[F]]): Option[String] = Some(records.head.topic)
  }

  /**
    * Pattern matching for ConsumerRecord partition
    */
  object Partition {
    def unapply[F[_]](record: ConsumerRecord[F]): Option[Int] = Some(record.partition)
  }

  /**
    * Pattern matching for ConsumerRecord offset
    */
  object Offset {
    def unapply[F[_]](record: ConsumerRecord[F]): Option[Long] = Some(record.offset)
  }

  type HeadersByKey[F[_]] = Map[String, Seq[Header[F]]]

  /**
    * Record headers extractor
    */
  object :? {

    def unapply[F[_]](record: Record[F]): Option[(Record[F], HeadersByKey[F])] =
      Some(record -> record.headers.toList.groupBy(_.key))
  }

  object +& {

    def unapply[F[_]](headers: HeadersByKey[F]): Option[(HeadersByKey[F], HeadersByKey[F])] =
      Some(headers -> headers)
  }

  case class HeaderByKey[A](key: String)(implicit D: Deserializer[A]) {

    def unapply[F[_]](headers: HeadersByKey[F]): Option[A] =
      for {
        v <- headers.get(key)
        h <- v.headOption
        a <- D.deserialize(h.value).toOption
      } yield a
  }

  case class OptionalHeaderByKey[A](key: String)(implicit D: Deserializer[A]) {
    def unapply[F[_]](headers: HeadersByKey[F]): Option[Option[A]] = Some(HeaderByKey[A](key).unapply(headers))
  }

  case class MultipleHeadersByKey(key: String) {

    def unapply[F[_]](headers: HeadersByKey[F]): Option[Seq[Header[F]]] = Some(headers.getOrElse(key, Seq.empty))
  }

  case class NelHeadersByKey(key: String) {

    def unapply[F[_]](headers: HeadersByKey[F]): Option[NonEmptyList[Header[F]]] =
      for {
        h   <- headers.get(key)
        nel <- NonEmptyList.fromList(h.toList)
      } yield nel
  }
}
