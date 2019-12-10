package io.kafka4s.effect.config

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig

case class ConsumerConfiguration private (bootstrapServers: Seq[String], groupId: String, properties: Properties)

object ConsumerConfiguration {

  def fromConfig: Either[Throwable, ConsumerConfiguration] =
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
