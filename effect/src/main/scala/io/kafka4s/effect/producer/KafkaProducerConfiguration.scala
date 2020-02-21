package io.kafka4s.effect.producer

import java.util.Properties

import io.kafka4s.effect.config._
import org.apache.kafka.clients.producer.ProducerConfig

case class KafkaProducerConfiguration private (bootstrapServers: Seq[String], properties: Properties)

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
