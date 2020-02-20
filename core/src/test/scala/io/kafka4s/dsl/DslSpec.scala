package io.kafka4s.dsl

import cats.Id
import cats.data.NonEmptyList
import io.kafka4s.common.{Header, Headers}
import io.kafka4s.consumer.ConsumerRecord
import io.kafka4s.test.UnitSpec

class DslSpec extends UnitSpec {

  val header = Header.of[Id]("foo" -> "bar")

  val record: ConsumerRecord[Id] =
    ConsumerRecord
      .of[Id](topic = "foo", key = "bar", value = "Hello, World!", partition = 0, offset = 1234L)
      .copy(headers = Headers[Id](List(header)))

  "Topic(name)" should "extract the topic name from a consumer record" in {
    val Topic(name) = record
    name shouldBe record.topic
  }

  it should "throw a MatchError when topic name does not match" in assertThrows[MatchError] {
    val Topic("bar") = record
  }

  "Partition(partition)" should "extract the partition from a consumer record" in {
    val Partition(partition) = record
    partition shouldBe record.partition
  }

  "Topic(name) & Partition(partition)" should "extract the partition from a consumer record" in {
    val Topic(_) & Partition(partition) = record
    partition shouldBe record.partition
  }

  it should "throw a MatchError when partition does not match" in assertThrows[MatchError] {
    val Topic(_) & Partition(1) = record
  }

  "Offset(partition)" should "extract the offset from a consumer record" in {
    val Offset(offset) = record
    offset shouldBe record.offset
  }

  "Topic(name) & Offset(offset)" should "extract the offset from a consumer record" in {
    val Topic(_) & Offset(offset) = record
    offset shouldBe record.offset
  }

  it should "throw a MatchError when offset does not match" in assertThrows[MatchError] {
    val Topic(_) & Offset(1000L) = record
  }

  "Topic(name) & Partition(partition) & Offset(offset)" should "extract the topic name, partition and offset from a consumer record" in {
    val Topic(name) & Partition(partition) & Offset(offset) = record
    name shouldBe record.topic
    partition shouldBe record.partition
    offset shouldBe record.offset
  }

  "BatchTopic(name)" should "extract the topic name from a non empty list of consumer records" in {
    val BatchTopic(name) = NonEmptyList.one(record)
    name shouldBe record.topic
  }

  behavior of ":?"

  final object FooHeader extends HeaderByKey[String]("foo")
  final object BarHeader extends HeaderByKey[String]("bar")
  final object MaybeFooHeader extends OptionalHeaderByKey[String]("foo")
  final object MaybeBarHeader extends OptionalHeaderByKey[String]("bar")
  final object MultiFooHeader extends MultipleHeadersByKey("foo")
  final object MultiBarHeader extends MultipleHeadersByKey("bar")
  final object NelFooHeader extends NelHeadersByKey("foo")
  final object NelBarHeader extends NelHeadersByKey("bar")

  "HeaderByKey[A]" should "extract a header by its key" in {
    val _ :? FooHeader(foo) = record
    foo shouldBe header.as[String]
  }

  it should "throw a MatchError when offset does not match" in assertThrows[MatchError] {
    val _ :? BarHeader(_) = record
  }

  "OptionalHeaderByKey[A]" should "maybe extract a header by key or return None instead" in {
    val _ :? MaybeFooHeader(foo) +& MaybeBarHeader(bar) = record
    foo shouldBe Some(header.as[String])
    bar shouldBe None
  }

  "MultipleHeadersByKey" should "extract multiple headers with the same key" in {
    val _ :? MultiFooHeader(foo) = record
    foo shouldBe Seq(header)
  }

  it should "return an empty list if no headers can be found" in {
    val _ :? MultiBarHeader(bar) = record
    bar shouldBe Seq.empty
  }

  "NelHeadersByKey" should "extract required multiple headers with the same key into a NonEmptyList" in {
    val _ :? NelFooHeader(foo) = record
    foo shouldBe NonEmptyList.one(header)
  }

  it should "throw a MatchError when there are no headers with given key" in assertThrows[MatchError] {
    val _ :? NelBarHeader(_) = record
  }
}
