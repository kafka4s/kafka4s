package io.kafka4s
import java.time.Instant

import cats.Show
import org.apache.kafka.clients.producer.RecordMetadata

final case class ProducerRecord[F[_]](topic: String,
                                      key: Array[Byte]           = Array.emptyByteArray,
                                      value: Array[Byte]         = Array.emptyByteArray,
                                      headers: Headers[F]        = Headers.empty[F],
                                      offset: Option[Long]       = None,
                                      partition: Option[Int]     = None,
                                      timestamp: Option[Instant] = None) extends Record[F] {

  def add(header: Header[F]*): ProducerRecord[F] = this.copy(headers = headers.add(header: _*))

  def addMetadata(metadata: RecordMetadata): ProducerRecord[F] =
    this.copy(
      offset = Some(metadata.offset()),
      partition = Some(metadata.partition()),
      timestamp = Some(Instant.ofEpochMilli(metadata.timestamp()))
    )

  def hasMetadata: Boolean = {
    for {
      _ <- offset
      _ <- partition
    } yield true
  }.getOrElse(false)
}

object ProducerRecord {
  def apply[F[_]](record: DefaultProducerRecord) =
    new ProducerRecord[F](
      topic      = record.topic(),
      key        = record.key(),
      value     = record.value(),
      headers   = Headers[F](record.headers()),
      partition = if (record.partition() == null) None else Some(record.partition()),
      timestamp = if (record.timestamp() == null) None else Some(Instant.ofEpochMilli(record.timestamp())),
    )

  implicit def show[F[_]]: Show[ProducerRecord[F]] = (record: ProducerRecord[F]) => {
    for {
      offset    <- record.offset
      partition <- record.partition
    } yield s"[${record.topic}-$partition@$offset]"
  }.getOrElse(record.toString())
}
