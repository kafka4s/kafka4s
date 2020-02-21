package io.kafka4s.effect.producer

import java.util.Properties

import cats.effect.{Concurrent, ExitCase}
import cats.implicits._
import io.kafka4s.producer.{DefaultProducer, DefaultProducerRecord}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.{Callback, RecordMetadata, KafkaProducer => ApacheKafkaProducer}
import org.apache.kafka.common.{Metric, MetricName, PartitionInfo, TopicPartition}

import scala.collection.JavaConverters._

class ProducerEffect[F[_]](producer: DefaultProducer)(implicit F: Concurrent[F]) {
  def initTransactions: F[Unit]  = F.delay(producer.initTransactions())
  def beginTransaction: F[Unit]  = F.delay(producer.beginTransaction())
  def commitTransaction: F[Unit] = F.delay(producer.commitTransaction())
  def abortTransaction: F[Unit]  = F.delay(producer.abortTransaction())

  def sendOffsetsToTransaction(offsets: Map[TopicPartition, OffsetAndMetadata], consumerGroupId: String): F[Unit] =
    F.delay(producer.sendOffsetsToTransaction(offsets.asJava, consumerGroupId))

  def transaction[A](fa: => F[A]): F[A] =
    for {
      _ <- F.delay(producer.beginTransaction())
      a <- F.guaranteeCase(fa) {
        case ExitCase.Completed                    => F.delay(producer.commitTransaction())
        case ExitCase.Error(_) | ExitCase.Canceled => F.delay(producer.abortTransaction())
      }
    } yield a

  def send(record: DefaultProducerRecord): F[RecordMetadata] = F.async { cb =>
    producer
      .send(
        record,
        new Callback {
          def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
            if (exception == null) cb(Right(metadata))
            else cb(Left(exception))
          }
        }
      )
    ()
  }

  def flush: F[Unit]                                      = F.delay(producer.flush())
  def partitionsFor(topic: String): F[Seq[PartitionInfo]] = F.delay(producer.partitionsFor(topic).asScala)
  def metrics: F[Map[MetricName, Metric]]                 = F.delay(producer.metrics().asScala.toMap)
  def close: F[Unit]                                      = F.delay(producer.close())
}

object ProducerEffect {

  def apply[F[_]](properties: Properties)(implicit F: Concurrent[F]): F[ProducerEffect[F]] =
    for {
      producer <- F.delay(new ApacheKafkaProducer[Array[Byte], Array[Byte]](properties))
    } yield new ProducerEffect[F](producer)
}
