package io.kafka4s.effect.consumer

import java.time.{Duration => JDuration}

import cats.effect.concurrent.Semaphore
import cats.implicits._
import cats.effect.{Blocker, ContextShift, Sync}
import io.kafka4s.consumer.{DefaultConsumer, DefaultConsumerRecord}
import org.apache.kafka.clients.consumer.{OffsetAndMetadata, OffsetAndTimestamp}
import org.apache.kafka.common.{Metric, MetricName, PartitionInfo, TopicPartition}

import scala.collection.JavaConverters._
import scala.concurrent.blocking
import scala.concurrent.duration.FiniteDuration
import scala.util.matching.Regex

class ConsumerF[F[_]] private (consumer: DefaultConsumer, semaphore: Semaphore[F], blocker: Blocker)(
  implicit F: Sync[F],
  CS: ContextShift[F]) {

  private def synced[A](thunk: => A): F[A] = semaphore.withPermit(blocker.delay(blocking(thunk)))

  def assign(partitions: Seq[TopicPartition]): F[Unit]                                = F.raiseError(???)
  def assignment: F[Set[TopicPartition]]                                              = F.raiseError(???)
  def beginningOffsets(partitions: Seq[TopicPartition]): F[Map[TopicPartition, Long]] = F.raiseError(???)
  def subscribe(topics: Seq[String]): F[Unit]                                         = F.raiseError(???)

  def subscribe(topics: Set[String]): F[Unit] =
    F.delay(consumer.subscribe(topics.asJava))

  def subscribe(regex: Regex): F[Unit] =
    F.delay(consumer.subscribe(regex.pattern))

  def committed(partition: TopicPartition): F[OffsetAndMetadata]                = F.raiseError(???)
  def endOffsets(partitions: Seq[TopicPartition]): F[Map[TopicPartition, Long]] = F.raiseError(???)
  def listTopics: F[Map[String, Seq[PartitionInfo]]]                            = F.raiseError(???)
  def metrics: F[Map[MetricName, Metric]]                                       = F.raiseError(???)

  def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Long]): F[Map[TopicPartition, OffsetAndTimestamp]] =
    F.raiseError(???)
  def partitionsFor(topic: String): F[Seq[PartitionInfo]] = F.raiseError(???)
  def position(partition: TopicPartition): F[Unit]        = F.raiseError(???)

  def seek(partition: TopicPartition, offset: Long): F[Unit]    = F.raiseError(???)
  def seekToBeginning(partitions: Seq[TopicPartition]): F[Unit] = F.raiseError(???)
  def seekToEnd(partitions: Seq[TopicPartition]): F[Unit]       = F.raiseError(???)
  def subscription: F[Seq[String]]                              = F.raiseError(???)
  def unsubscribe: F[Unit]                                      = F.raiseError(???)

  def pause(partitions: Seq[TopicPartition]): F[Unit]  = F.delay(consumer.pause(partitions.asJava))
  def resume(partitions: Seq[TopicPartition]): F[Unit] = F.delay(consumer.resume(partitions.asJava))
  def paused: F[Set[TopicPartition]]                   = F.delay(consumer.paused().asScala.toSet)

  def wakeup: F[Unit] =
    F.delay(consumer.wakeup())

  def close: F[Unit] =
    synced(consumer.close())

  def close(timeout: FiniteDuration): F[Unit] =
    synced(consumer.close(JDuration.ofMillis(timeout.toMillis)))

  def commit(): F[Unit] =
    synced(consumer.commitSync())

  def commit(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] =
    synced(consumer.commitSync(offsets.asJava))

  def poll(timeout: FiniteDuration): F[Iterable[DefaultConsumerRecord]] =
    for {
      records <- synced(consumer.poll(JDuration.ofMillis(timeout.toMillis)))
    } yield records.asScala
}
