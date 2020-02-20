package io.kafka4s.effect.config

import java.util.Properties

import org.apache.kafka.clients.producer.ProducerConfig

class ProducerConfiguration private (bootstrapId: Seq[String], properties: Properties) {

  def toProducer: Properties = {
    properties.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    properties.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all")
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                   "org.apache.kafka.common.serialization.ByteArraySerializer")
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                   "org.apache.kafka.common.serialization.ByteArraySerializer")
    properties
  }
}

object ProducerConfiguration {

  def load: Either[Throwable, ProducerConfiguration] =
    for {
      properties <- configToProperties("kafka4s.producer")
      config     <- fromProperties(properties)
    } yield config

  def fromProperties(properties: Properties): Either[Throwable, ProducerConfiguration] =
    for {
      bootstrapServers <- properties.getter[String](ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)
    } yield new ProducerConfiguration(bootstrapServers.split(raw",").map(_.trim), properties)
}
