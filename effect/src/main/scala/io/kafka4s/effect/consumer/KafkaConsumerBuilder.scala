package io.kafka4s.effect.consumer

import java.util.Properties

import cats.ApplicativeError
import cats.effect.{Resource, Sync}
import io.kafka4s.RecordConsumer
import io.kafka4s.consumer._

import scala.concurrent.duration._
import scala.util.matching.Regex

case class KafkaConsumerBuilder[F[_]](pollTimeout: FiniteDuration,
                                      properties: Properties,
                                      subscription: Subscription,
                                      recordConsumer: RecordConsumer[F]) {

  type Self = KafkaConsumerBuilder[F]

  def withTopics(topics: String*): Self =
    copy(subscription = Subscription.Topics(topics.toSet))

  def withTopics(topics: Set[String]): Self =
    copy(subscription = Subscription.Topics(topics))

  def withPattern(regex: Regex): Self =
    copy(subscription = Subscription.Pattern(regex))

  def withPollTimeout(duration: FiniteDuration): Self =
    copy(pollTimeout = duration)

  def withConsumer(consumer: Consumer[F])(implicit F: ApplicativeError[F, Throwable]): Self =
    copy(recordConsumer = consumer.orNotFound)

  def resource: Resource[F, KafkaConsumer[F]] = ???
}

object KafkaConsumerBuilder {

  def apply[F[_]: Sync](): KafkaConsumerBuilder[F] =
    KafkaConsumerBuilder[F](pollTimeout    = 100.millis,
                            properties     = new Properties(),
                            subscription   = Subscription.Empty,
                            recordConsumer = Consumer.empty[F].orNotFound)
}
