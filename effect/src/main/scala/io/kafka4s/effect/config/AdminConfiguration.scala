package io.kafka4s.effect.config

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig

case class AdminConfiguration(bootstrapServers: Seq[String], properties: Properties)

object AdminConfiguration {

  def fromConfig: Either[Throwable, AdminConfiguration] =
    for {
      properties <- configToProperties("kafka4s.consumer")
      config     <- fromProperties(properties)
    } yield config

  def fromProperties(properties: Properties): Either[Throwable, AdminConfiguration] =
    for {
      bootstrapServers <- properties.getter[String](ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)
    } yield AdminConfiguration(bootstrapServers.split(raw",").map(_.trim), properties)
}
