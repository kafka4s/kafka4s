package io.kafka4s.consumer

import cats.Id
import cats.data.NonEmptyList
import cats.implicits._
import io.kafka4s.dsl._
import io.kafka4s.test.UnitSpec

class BatchConsumerSpec extends UnitSpec {
  type Test[A] = Either[Throwable, A]

  def eitherTest[A](fa: Test[A]): A = {
    fa.fold(ex => fail(ex.getMessage), identity)
  }

  "$.of" should "wrap a partial function in a Kleisli that may consume a record" in {
    val consumer: BatchConsumer[Id] = BatchConsumer.of[Id] {
      case BatchTopic("my-topic") => ()
    }

    for {
      en <- ConsumerRecord.of[Id]("hello-topic", "Hello, World!")
      pt <- ConsumerRecord.of[Id]("hello-topic", "Olá, Mundo!")
      ru <- ConsumerRecord.of[Id]("hello-topic", "Привет, мир")
      ch <- ConsumerRecord.of[Id]("topic-hello", "你好，世界")
      batch1 = NonEmptyList.fromListUnsafe(List(en, pt, ru))
      batch2 = NonEmptyList.fromListUnsafe(List(ch))
      a <- consumer.apply(batch1).value
      b <- consumer.apply(batch2).value
    } yield {
      a shouldBe Some(())
      b shouldBe None
    }
  }

  behavior of "BatchRecordConsumer[F]"

  ".orNotFound" should "transform the Consumer in a total function that lifts the record in a Return" in {
    val consumer = BatchConsumer
      .of[Id] {
        case BatchTopic("hello-topic") => ()
      }
      .orNotFound

    for {
      en <- ConsumerRecord.of[Id]("hello-topic", "Hello, World!")
      a  <- consumer.apply(NonEmptyList.one(en))

      cn <- ConsumerRecord.of[Id]("hello-topic", "你好，世界")
      b  <- consumer.apply(NonEmptyList.one(cn))
    } yield {
      a.records.head.as[String] shouldBe "Hello, World!"
      b.records.head.as[String] shouldBe "你好，世界"
    }
  }

  it should "lift any exception into the Err data type" in eitherTest {
    val ex = new Exception("Boom!")

    val consumer = BatchConsumer
      .of[Test] {
        case BatchTopic("boom") => Left(ex)
      }
      .orNotFound

    for {
      msg <- ConsumerRecord.of[Test]("boom", "Hello, World!")
      err <- consumer.apply(NonEmptyList.one(msg))
    } yield {
      err shouldBe BatchReturn.Err(NonEmptyList.one(msg), ex)
    }
  }

  it should "return a NotFound data type if cannot find a consumer to execute" in eitherTest {
    val consumer = BatchConsumer.empty[Test].orNotFound

    for {
      msg <- ConsumerRecord.of[Test]("hello", "Hello, World!")
      err <- consumer.apply(NonEmptyList.one(msg))
    } yield {
      err shouldBe BatchReturn.Err(NonEmptyList.one(msg), TopicNotFound("hello"))
    }
  }
}
