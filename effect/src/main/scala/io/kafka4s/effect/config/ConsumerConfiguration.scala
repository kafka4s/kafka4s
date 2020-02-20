package io.kafka4s.effect.config

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig

case class ConsumerConfiguration private (bootstrapServers: Seq[String], groupId: String, properties: Properties) {

  def toConsumer: Properties = {
    properties.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                   "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                   "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    properties
  }
}

object ConsumerConfiguration {

  def load: Either[Throwable, ConsumerConfiguration] =
    for {
      properties <- configToProperties("kafka4s.consumer")
      config     <- fromProperties(properties)
    } yield config

  def fromProperties(properties: Properties): Either[Throwable, ConsumerConfiguration] =
    for {
      bootstrapServers <- properties.getter[String](ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)
      groupId          <- properties.getter[String](ConsumerConfig.GROUP_ID_CONFIG)
    } yield ConsumerConfiguration(bootstrapServers.split(raw",").map(_.trim), groupId, properties)
}
