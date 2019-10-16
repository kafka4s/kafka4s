package io.kafka4s.effect.log

final case class Message(level: Level, value: String, ex: Option[Throwable])
