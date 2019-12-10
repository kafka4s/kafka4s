package io.kafka4s.effect.config

import java.util.Properties

import org.apache.kafka.clients.producer.ProducerConfig

class ProducerConfiguration private (bootstrapId: Seq[String])

object ProducerConfiguration {

  def fromConfig: Either[Throwable, ProducerConfiguration] =
    for {
      properties <- configToProperties("kafka4s.producer")
      config     <- fromProperties(properties)
    } yield config

  def fromProperties(properties: Properties): Either[Throwable, ProducerConfiguration] =
    for {
      bootstrapServers <- properties.getter[String](ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)
    } yield new ProducerConfiguration(bootstrapServers.split(raw",").map(_.trim))
}
