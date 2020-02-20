package io.kafka4s.effect.consumer

import java.time.{Duration => JDuration}
import java.util.Properties

import cats.effect._
import cats.implicits._
import io.kafka4s.consumer.{DefaultConsumer, DefaultConsumerRecord}
import org.apache.kafka.clients.consumer.{OffsetAndMetadata, OffsetAndTimestamp, KafkaConsumer => ApacheKafkaConsumer}
import org.apache.kafka.common.{Metric, MetricName, PartitionInfo, TopicPartition}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.matching.Regex

class ConsumerEffect[F[_]] private (consumer: DefaultConsumer, blocker: Blocker, threadSafe: ThreadSafeBlocker[F])(
  implicit F: Sync[F],
  CS: ContextShift[F]) {

  def metrics: F[Map[MetricName, Metric]] = F.delay(consumer.metrics().asScala.toMap)

  def assign(partitions: Seq[TopicPartition]): F[Unit] = F.delay(consumer.assign(partitions.asJava))
  def subscribe(topics: Seq[String]): F[Unit]          = F.delay(consumer.subscribe(topics.asJava))
  def subscribe(regex: Regex): F[Unit]                 = F.delay(consumer.subscribe(regex.pattern))
  def unsubscribe: F[Unit]                             = F.delay(consumer.unsubscribe())

  def pause(partitions: Seq[TopicPartition]): F[Unit]  = F.delay(consumer.pause(partitions.asJava))
  def resume(partitions: Seq[TopicPartition]): F[Unit] = F.delay(consumer.resume(partitions.asJava))
  def paused: F[Set[TopicPartition]]                   = F.delay(consumer.paused().asScala.toSet)
  def wakeup: F[Unit]                                  = F.delay(consumer.wakeup())

  // Blocking operations
  def subscription: F[Set[String]]       = blocker.delay(consumer.subscription().asScala.toSet)
  def assignment: F[Set[TopicPartition]] = blocker.delay(consumer.assignment().asScala.toSet)

  def listTopics: F[Map[String, Seq[PartitionInfo]]] =
    blocker.delay(consumer.listTopics().asScala.toMap.mapValues(_.asScala.toSeq))

  def position(partition: TopicPartition): F[Long]               = blocker.delay(consumer.position(partition))
  def committed(partition: TopicPartition): F[OffsetAndMetadata] = blocker.delay(consumer.committed(partition))

  def beginningOffsets(partitions: Seq[TopicPartition]): F[Map[TopicPartition, Long]] =
    blocker.delay(Map(consumer.beginningOffsets(partitions.asJavaCollection).asScala.mapValues(Long2long).toSeq: _*))

  def endOffsets(partitions: Seq[TopicPartition]): F[Map[TopicPartition, Long]] =
    blocker.delay(Map(consumer.endOffsets(partitions.asJavaCollection).asScala.mapValues(Long2long).toSeq: _*))

  def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Long]): F[Map[TopicPartition, OffsetAndTimestamp]] =
    blocker.delay(
      Map(
        consumer
          .offsetsForTimes(timestampsToSearch.mapValues(long2Long).asJava)
          .asScala
          .toSeq: _*))

  def partitionsFor(topic: String): F[Seq[PartitionInfo]] =
    blocker.delay(consumer.partitionsFor(topic).asScala.toVector)

  def seek(partition: TopicPartition, offset: Long): F[Unit] = blocker.delay(consumer.seek(partition, offset))

  def seekToBeginning(partitions: Seq[TopicPartition]): F[Unit] =
    blocker.delay(consumer.seekToBeginning(partitions.asJavaCollection))

  def seekToEnd(partitions: Seq[TopicPartition]): F[Unit] =
    blocker.delay(consumer.seekToEnd(partitions.asJavaCollection))

  // Blocking and non-thread safe operations
  def close(timeout: FiniteDuration = 30.seconds): F[Unit] =
    threadSafe.delay(consumer.close(JDuration.ofMillis(timeout.toMillis)))

  def commit(): F[Unit] =
    threadSafe.delay(consumer.commitSync())

  def commit(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] =
    threadSafe.delay(consumer.commitSync(offsets.asJava))

  def poll(timeout: FiniteDuration): F[Iterable[DefaultConsumerRecord]] =
    threadSafe.delay(consumer.poll(JDuration.ofMillis(timeout.toMillis)).asScala)
}

object ConsumerEffect {

  def apply[F[_]](properties: Properties, blocker: Blocker)(implicit F: Concurrent[F],
                                                            CS: ContextShift[F]): F[ConsumerEffect[F]] =
    for {
      consumer   <- F.delay(new ApacheKafkaConsumer[Array[Byte], Array[Byte]](properties))
      threadSafe <- ThreadSafeBlocker[F](blocker)
    } yield new ConsumerEffect(consumer, blocker, threadSafe)
}
