package io.kafka4s.effect.producer

import java.util.Properties

import io.kafka4s.effect.config._
import org.apache.kafka.clients.producer.ProducerConfig

class KafkaProducerConfiguration private (bootstrapId: Seq[String], properties: Properties) {

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

object KafkaProducerConfiguration {

  def load: Either[Throwable, KafkaProducerConfiguration] =
    for {
      properties <- configToProperties("kafka4s.producer")
      config     <- loadFrom(properties)
    } yield config

  def loadFrom(properties: Properties): Either[Throwable, KafkaProducerConfiguration] =
    for {
      bootstrapServers <- properties.getter[String](ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)
    } yield new KafkaProducerConfiguration(bootstrapServers.split(raw",").map(_.trim), properties)
}
