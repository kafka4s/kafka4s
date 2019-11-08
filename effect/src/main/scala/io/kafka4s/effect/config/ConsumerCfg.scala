package io.kafka4s.effect.config

import scala.concurrent.duration.FiniteDuration

final case class ConsumerCfg(pollTimeout: FiniteDuration)
