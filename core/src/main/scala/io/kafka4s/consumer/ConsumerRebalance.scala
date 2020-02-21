package io.kafka4s.consumer

import org.apache.kafka.common.TopicPartition

sealed trait ConsumerRebalance {
  def partitions: Seq[TopicPartition]
}

object ConsumerRebalance {

  final case class PartitionsAssigned(partitions: Seq[TopicPartition]) extends ConsumerRebalance
  final case class PartitionsRevoked(partitions: Seq[TopicPartition]) extends ConsumerRebalance

}
