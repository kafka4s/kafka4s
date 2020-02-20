package io.kafka4s.consumer

import cats.Id
import cats.implicits._
import io.kafka4s.dsl._
import io.kafka4s.test.UnitSpec

class ConsumerSpec extends UnitSpec {
  type Test[A] = Either[Throwable, A]

  def eitherTest[A](fa: Test[A]): A = {
    fa.fold(ex => fail(ex.getMessage), identity)
  }

  "$.of" should "wrap a partial function in a Kleisli that may consume a record" in {
    val consumerEn: Consumer[Id] = Consumer.of[Id] {
      case _ @ Topic("my-topic-en") => ()
    }

    val consumerPt: Consumer[Id] = Consumer.of[Id] {
      case _ @ Topic("my-topic-pt") => ()
    }

    val consumer: Consumer[Id] = consumerEn <+> consumerPt

    for {
      en <- ConsumerRecord.of[Id]("my-topic-en", "Hello, World!")
      pt <- ConsumerRecord.of[Id]("my-topic-pt", "Olá, Mundo!")
      ru <- ConsumerRecord.of[Id]("my-topic-ru", "Привет, мир")
      a  <- consumer.apply(en).value
      b  <- consumer.apply(pt).value
      c  <- consumer.apply(ru).value
    } yield {
      a shouldBe Some(())
      b shouldBe Some(())
      c shouldBe None
    }
  }

  behavior of "RecordConsumer[F]"

  ".orNotFound" should "transform the Consumer in a total function that lifts the record in a Return" in {
    val consumer = Consumer
      .of[Id] {
        case _ @Topic("my-topic-en") => ()
        case _ @Topic("my-topic-ch") => ()
      }
      .orNotFound

    for {
      en <- ConsumerRecord.of[Id]("my-topic-en", "Hello, World!")
      a  <- consumer.apply(en)

      cn <- ConsumerRecord.of[Id]("my-topic-ch", "你好，世界")
      b  <- consumer.apply(cn)
    } yield {
      a.record.as[String] shouldBe "Hello, World!"
      b.record.as[String] shouldBe "你好，世界"
    }
  }

  it should "lift any exception into the Err data type" in eitherTest {
    val ex = new Exception("Boom!")

    val consumer = Consumer
      .of[Test] {
        case Topic("boom") => Left(ex)
      }
      .orNotFound

    for {
      msg <- ConsumerRecord.of[Test]("boom", "Hello, World!")
      err <- consumer.apply(msg)
    } yield {
      err shouldBe Return.Err(msg, ex)
    }
  }

  it should "return a NotFound data type if cannot find a consumer to execute" in eitherTest {
    val consumer = Consumer.empty[Test].orNotFound

    for {
      msg <- ConsumerRecord.of[Test]("hello", "Hello, World!")
      err <- consumer.apply(msg)
    } yield {
      err shouldBe Return.Err(msg, TopicNotFound("hello"))
    }
  }
}
