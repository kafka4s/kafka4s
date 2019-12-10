package io.kafka4s.consumer

import scala.util.matching.Regex

sealed trait Subscription

object Subscription {
  final case object Empty extends Subscription
  final case class Topics(topics: Set[String]) extends Subscription
  final case class Pattern(regex: Regex) extends Subscription
}
