package io.kafka4s

import io.kafka4s.consumer.ConsumerImplicits
import io.kafka4s.dsl.DslConsumer
import io.kafka4s.serdes.SerdeImplicits

object implicits extends DslConsumer with ConsumerImplicits with SerdeImplicits
