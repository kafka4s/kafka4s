package io.kafka4s.consumer

final case class TopicNotFound(topic: String) extends RuntimeException(s"Consumer not found for topic '$topic'")
