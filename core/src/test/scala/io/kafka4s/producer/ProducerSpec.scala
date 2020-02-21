package io.kafka4s.producer

import java.time.Instant

import cats.Id
import cats.data.Kleisli
import Return.Ack
import io.kafka4s.serdes._
import io.kafka4s.test.UnitSpec

class ProducerSpec extends UnitSpec {

  val topic = "hello-world"
  val id    = 1L
  val hello = "Hello, World!"

  val producer = new Producer[Id] {

    // Just return an ack for testing
    def send1: Kleisli[Id, ProducerRecord[Id], Return[Id]] =
      Kleisli(Ack(_, 0, Some(0L), Some(Instant.now())).asInstanceOf[Id[Return[Id]]])
  }

  ".send" should "create a record from a topic and value" in {
    val ack = producer.send(topic, hello)
    ack.record.topic shouldBe topic
    ack.record.as[String] shouldBe hello
  }

  it should "create a record from a tupled topic" in {
    val ack = producer.send(topic -> hello)
    ack.record.topic shouldBe topic
    ack.record.as[String] shouldBe hello
  }

  it should "create a record from topic, key and value" in {
    val ack = producer.send(topic, id, hello)
    ack.record.topic shouldBe topic
    ack.record.key[Long] shouldBe id
    ack.record.as[String] shouldBe hello
  }
}
