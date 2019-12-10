package io.kafka4s.effect.consumer

import cats.effect.concurrent.Ref
import cats.effect.{CancelToken, Concurrent, Resource}
import cats.implicits._
import io.kafka4s.RecordConsumer
import io.kafka4s.consumer.{ConsumerRecord, DefaultConsumerRecord, Return}
import io.kafka4s.effect.config.ConsumerConfiguration
import io.kafka4s.effect.log.Log
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

private[kafka4s] class KafkaConsumer[F[_]] private (
  config: ConsumerConfiguration,
  consumer: ConsumerEffect[F],
  recordConsumer: RecordConsumer[F])(implicit F: Concurrent[F], L: Log[F]) {

  private def commit(records: Seq[ConsumerRecord[F]]): F[Unit] =
    if (records.isEmpty) F.unit
    else
      for {
        offsets <- F.pure(records.toList.map { record =>
          new TopicPartition(record.topic, record.partition) -> new OffsetAndMetadata(record.offset + 1L)
        })
        _ <- consumer.commit(offsets.toMap)
        _ <- L.debug(s"Committing records [${records.map(_.show).mkString(", ")}]")
      } yield ()

  private def consume1(record: DefaultConsumerRecord): F[Return[F]] =
    for {
      r <- recordConsumer.apply(ConsumerRecord[F](record))
      _ <- r match {
        case Return.Ack(r)     => L.debug(s"Record [${r.show}] processed successfully")
        case Return.Err(r, ex) => L.error(s"Error processing [${r.show}]", ex)
      }
    } yield r

  private def consume(records: Iterable[DefaultConsumerRecord]): F[Unit] =
    for {
      r <- records.toVector.traverse(consume1)
      a = r
        .filter {
          case Return.Ack(_) => true
          case _             => false
        }
        .map(_.record)
      _ <- commit(a)
    } yield ()

  private def fetch(exitSignal: Ref[F, Boolean]): F[Unit] = {
    val loop = for {
      records <- consumer.poll(???) // config.pollTimeout
      _       <- if (records.isEmpty) F.unit else consume(records)
      exit    <- exitSignal.get
    } yield exit

    loop.flatMap(exit => if (exit) F.unit else fetch(exitSignal))
  }

  def start: F[CancelToken[F]] =
    for {
      exitSignal <- Ref.of[F, Boolean](false)
      fiber      <- F.start(fetch(exitSignal))
    } yield exitSignal.set(true) >> fiber.join

  def resource: Resource[F, Unit] =
    Resource.make(start)(identity).void
}
