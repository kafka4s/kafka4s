package io.kafka4s.middlewares.dlq

import cats.data.Kleisli
import cats.implicits._
import io.kafka4s.consumer.{Consumer, ConsumerRecord, Return => ConsumerReturn}
import io.kafka4s.dsl._
import io.kafka4s.producer.{Producer, ProducerRecord, Return => ProducerReturn}
import io.kafka4s.test.UnitSpec

class DeadLetterQueueSpec extends UnitSpec { self =>

  type Test[A] = Either[Throwable, A]

  val send1 = mockFunction[ProducerRecord[Test], Unit]

  val producer: Producer[Test] = new Producer[Test] {

    def send1: Kleisli[Test, ProducerRecord[Test], ProducerReturn[Test]] = Kleisli { record =>
      Either.catchNonFatal(self.send1(record)) match {
        case Right(_) => Right(ProducerReturn.Ack(record, partition = 1, offset = None, timestamp = None))
        case Left(ex) => Right(ProducerReturn.Err(record, ex))
      }
    }
  }

  val consumer = Consumer.of[Test] {
    case Topic("boom") => Either.catchNonFatal(throw new Error("Boom!")).void
    case _             => Right(())
  }

  def eitherTest[A](fa: Test[A]): A = {
    fa.fold(ex => fail(ex.getMessage), identity)
  }

  it should
    """|recover from a error in the original consumer by producing a dead letter message
       |containing the exception message and stack trace in the headers
       |""".stripMargin in eitherTest {
    val dlq = DeadLetterQueue(producer)(consumer).orNotFound
    send1
      .expects(where { record: ProducerRecord[Test] =>
        record.topic.endsWith("-dlq") &&
        record.header[String]("X-Exception-Message") == Right(Some("Error: Boom!")) &&
        record.header[String]("X-Stack-Trace").map(_.nonEmpty) == Right(true)
      })
      .returns(())
      .once()

    for {
      record1 <- ConsumerRecord.of[Test]("foo" -> "I will be back")
      record2 <- ConsumerRecord.of[Test]("boom" -> "You are terminated!")
      ack1    <- dlq.apply(record1)
      ack2    <- dlq.apply(record2)
    } yield {
      ack1 shouldBe ConsumerReturn.Ack(record1)
      ack2 shouldBe ConsumerReturn.Ack(record2)
    }
  }
}
