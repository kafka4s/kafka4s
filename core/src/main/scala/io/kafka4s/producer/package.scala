package io.kafka4s

import org.apache.kafka.clients.producer.{Producer => ApacheProducer, ProducerRecord => ApacheProducerRecord}

package object producer {
  private[kafka4s] type DefaultProducer       = ApacheProducer[Array[Byte], Array[Byte]]
  private[kafka4s] type DefaultProducerRecord = ApacheProducerRecord[Array[Byte], Array[Byte]]
}
