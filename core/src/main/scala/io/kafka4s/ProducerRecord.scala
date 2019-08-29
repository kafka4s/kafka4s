package io.kafka4s
import java.time.Instant

import cats.Show
import io.kafka4s.ProducerRecord.Metadata
import org.apache.kafka.clients.producer.RecordMetadata

final case class ProducerRecord[F[_]](topic: String,
                                      key: Array[Byte]           = Array.emptyByteArray,
                                      value: Array[Byte]         = Array.emptyByteArray,
                                      headers: Headers[F]        = Headers.empty[F],
                                      partition: Option[Int]     = None,
                                      metadata: Option[Metadata] = None) extends Record[F] {

  def add(header: Header[F]*): ProducerRecord[F] = this.copy(headers = headers.add(header: _*))

  def add(metadata: RecordMetadata): ProducerRecord[F] = this.copy(metadata = Some(
    ProducerRecord.Metadata(
      offset    = metadata.offset(),
      partition = metadata.partition(),
      timestamp = if (metadata.hasTimestamp) Instant.ofEpochMilli(metadata.timestamp()) else Instant.now()
    )
  ))
}

object ProducerRecord {
  final case class Metadata(offset: Long, partition: Int, timestamp: Instant)

  def apply[F[_]](record: DefaultProducerRecord) =
    new ProducerRecord[F](
      topic      = record.topic(),
      key        = record.key(),
      value     = record.value(),
      headers   = Headers[F](record.headers()),
      partition = if (record.partition() == null) None else Some(record.partition())
    )

  implicit def show[F[_]]: Show[ProducerRecord[F]] = (record: ProducerRecord[F]) =>
    record.metadata.map(m => s"[${record.topic}-${m.partition}@${m.offset}]").getOrElse(record.toString())
}
