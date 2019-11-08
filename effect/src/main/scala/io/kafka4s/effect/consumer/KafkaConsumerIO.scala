package io.kafka4s.effect.consumer

import cats.data.NonEmptyList
import cats.effect.{CancelToken, Concurrent, Resource, Sync}
import cats.effect.concurrent.{Ref, Semaphore}
import cats.implicits._
import io.kafka4s.RecordConsumer
import io.kafka4s.consumer.Return
import io.kafka4s.consumer.{ConsumerRecord, DefaultConsumerRecord}
import io.kafka4s.effect.config.ConsumerCfg
import io.kafka4s.effect.log.Log
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

private[kafka4s] class KafkaConsumerIO[F[_]] private (config: ConsumerCfg,
                                                      consumer: ConsumerF[F],
                                                      consumerFn: RecordConsumer[F],
                                                      guard: Semaphore[F])(implicit F: Concurrent[F], L: Log[F]) {

  private def commit(records: Seq[DefaultConsumerRecord]): F[Unit] =
    if (records.isEmpty) F.unit
    else
      for {
        offsets <- F.pure(records.toList.map { record =>
          new TopicPartition(record.topic(), record.partition()) -> new OffsetAndMetadata(record.offset() + 1L)
        })
        _ <- consumer.commit(offsets.toMap)
        _ <- L.debug(s"Committing records [${}]")
      } yield ()

  private def consume(records: Iterable[DefaultConsumerRecord]): F[Unit] =
    for {
      r <- records.toVector.map(ConsumerRecord[F]).traverse(consumerFn.apply)
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
      records <- consumer.poll(config.pollTimeout)
      _       <- if (records.isEmpty) F.unit else consume(records)
      exit    <- exitSignal.get
    } yield exit

    loop.flatMap(e => if (e) F.unit else fetch(exitSignal))
  }

  def start: F[CancelToken[F]] =
    for {
      exit  <- Ref.of[F, Boolean](false)
      fiber <- F.start(fetch(exit))
    } yield exit.set(true) >> fiber.join

  def resource: Resource[F, Unit] = Resource.make(start)(identity).void
}
