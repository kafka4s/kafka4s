package io.kafka4s.effect.consumer

import java.util.Properties

import io.kafka4s.effect.config._
import org.apache.kafka.clients.consumer.ConsumerConfig

case class KafkaConsumerConfiguration private (bootstrapServers: Seq[String], groupId: String, properties: Properties)

object KafkaConsumerConfiguration {

  def load: Either[Throwable, KafkaConsumerConfiguration] =
    for {
      properties <- configToProperties("kafka4s.consumer")
      config     <- loadFrom(properties)
    } yield config

  def loadFrom(properties: Properties): Either[Throwable, KafkaConsumerConfiguration] =
    for {
      bootstrapServers <- properties.getter[String](ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)
      groupId          <- properties.getter[String](ConsumerConfig.GROUP_ID_CONFIG)
    } yield KafkaConsumerConfiguration(bootstrapServers.split(raw",").map(_.trim), groupId, properties)
}
