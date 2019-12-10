package io.kafka4s.producer

import cats.Id
import io.kafka4s.test.UnitSpec

class ProducerRecordSpec extends UnitSpec {
  "$.of" should "create an ProducerRecord instance from a topic and message tuple" in {
    val record = ProducerRecord.of[Id]("my-topic" -> "message")
    record.topic shouldBe "my-topic"
    record.as[String] shouldBe "message"
    record.key[Option[String]] shouldBe None
    record.partition shouldBe None
  }

  it should "create an ProducerRecord instance from a topic and message arguments" in {
    val record = ProducerRecord.of[Id]("my-topic", "message")
    record.topic shouldBe "my-topic"
    record.as[String] shouldBe "message"
    record.key[Option[String]] shouldBe None
    record.partition shouldBe None
  }

  it should "create an ProducerRecord instance from a topic, key and message" in {
    val record = ProducerRecord.of[Id]("my-topic", "foo", "bar")
    record.topic shouldBe "my-topic"
    record.key[String] shouldBe "foo"
    record.as[String] shouldBe "bar"
    record.partition shouldBe None
  }

  it should "create an ProducerRecord instance from a topic, key, message and partition" in {
    val record = ProducerRecord.of[Id]("my-topic", "foo", "bar", 0)
    record.topic shouldBe "my-topic"
    record.key[String] shouldBe "foo"
    record.as[String] shouldBe "bar"
    record.partition shouldBe Some(0)
  }

  ".put" should "" in {}
}
