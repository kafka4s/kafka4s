package io.kafka4s.dsl

import cats.Id
import cats.data.NonEmptyList
import cats.implicits._
import io.kafka4s.consumer.ConsumerRecord
import io.kafka4s.test.UnitSpec

class DslSpec extends UnitSpec {

  val record: ConsumerRecord[Id] =
    ConsumerRecord.of[Id](topic = "foo", key = "bar", value = "Hello, World!", partition = 0, offset = 1234L)

  "Topic(name)" should "extract the topic name from a consumer record" in {
    val Topic(name) = record
    name shouldBe record.topic
  }

  it should "return MatchError when topic name does not match" in assertThrows[MatchError] {
    val Topic("bar") = record
  }

  "Topic(name) / partition" should "extract the partition from a consumer record" in {
    val Topic(_) & Partition(partition) = record
    partition shouldBe record.partition
  }

  it should "return MatchError when partition does not match" in assertThrows[MatchError] {
    val Topic(_) & Partition(1) = record
  }

  "Topic(name) :@ offset" should "extract the offset from a consumer record" in {
    val Topic(_) & Offset(offset) = record
    offset shouldBe record.offset
  }

  it should "return MatchError when offset does not match" in assertThrows[MatchError] {
    val Topic(_) & Offset(1000L) = record
  }
  
  "Topic(name) / partition :@ offset" should "extract the topic name, partition and offset from a consumer record" in {
    val Topic(name) & Partition(partition) & Offset(offset) = record
    name shouldBe record.topic
    partition shouldBe record.partition
    offset shouldBe record.offset
  }

  "BatchTopic(name)" should "extract the topic name from a non empty list of consumer records" in {
    val BatchTopic(name) = NonEmptyList.one(record)
    name shouldBe record.topic
  }
}

