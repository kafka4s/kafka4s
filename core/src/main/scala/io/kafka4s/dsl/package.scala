package io.kafka4s

import cats.data.NonEmptyList
import io.kafka4s.consumer.ConsumerRecord
import io.kafka4s.serdes.SerdeImplicits

package object dsl extends DslImplicits with SerdeImplicits {

  /**
   * Pattern matching for Record topic
   */
  object Topic {
    def unapply[F[_]](record: Record[F]): Option[String] = Some(record.topic)
  }

  /**
   * Pattern matching for a batch of Record topic
   */
  object BatchTopic {
    def unapply[F[_]](records: NonEmptyList[Record[F]]): Option[String] = Some(records.head.topic)
  }

  /**
   * Pattern matching for ConsumerRecord partition
   */
  object - {
    def unapply[F[_]](record: ConsumerRecord[F]): Option[(Record[F], Int)] = Some(record -> record.partition)
  }

  /**
   * Pattern matching for ConsumerRecord offset
   */
  object at {
    def unapply[F[_]](record: ConsumerRecord[F]): Option[(Record[F], Long)] = Some(record -> record.offset)
  }
}
