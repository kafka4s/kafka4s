package io.kafka4s.producer

import java.time.Instant

import cats.Id
import cats.data.Kleisli
import io.kafka4s.producer.Return.Ack
import io.kafka4s.serdes._
import io.kafka4s.test.UnitSpec

class ProducerSpec extends UnitSpec {

  val topic = "hello-world"
  val id = 1L
  val hello = "Hello, World!"
  
  val producer = new Producer[Id] {
    // Just return an ack for testing
    def send1: Kleisli[Id, ProducerRecord[Id], Return[Id]] = Kleisli(Ack(_, 0, 0L, Instant.now()).asInstanceOf[Id[Return[Id]]])
  }

  ".send" should "create a record from a topic and value and add the class name header" in {
    val ack = producer.send(topic, hello)
    ack.record.topic shouldBe topic
    ack.record.as[String] shouldBe hello
//    ack.record.headers.values should contain (Header.of[Id](producer.valueTypeHeaderName, "java.lang.String"))
  }

  it should "create a record from a tupled topic and value and add the value class name header" ignore {
    val ack = producer.send(topic -> hello)
    ack.record.topic shouldBe topic
    ack.record.as[String] shouldBe hello
//    ack.record.headers
  }

  it should "create a record from topic, key and value and add the class name headers" ignore {
    val ack = producer.send(topic, id, hello)
    ack.record.topic shouldBe topic
    ack.record.keyAs[Long] shouldBe id
    ack.record.as[String] shouldBe hello

//    ack.record.headers.values should contain allOf (
//      Header.of[Id](producer.keyTypeHeaderName, "scala.Long"),
//      Header.of[Id](producer.valueTypeHeaderName, "java.lang.String")
//    )
  }
}
