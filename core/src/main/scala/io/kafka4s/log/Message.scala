package io.kafka4s.log

final case class Message(level: Level, value: String, ex: Option[Throwable])
