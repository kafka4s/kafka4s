package io.kafka4s.effect

import cats.effect.concurrent.Deferred
import cats.effect.{ContextShift, IO, Resource, Timer}
import io.kafka4s.{producer, Producer}
import io.kafka4s.consumer._
import io.kafka4s.effect.admin.KafkaAdminBuilder
import io.kafka4s.effect.consumer.KafkaConsumerBuilder
import io.kafka4s.effect.producer.KafkaProducerBuilder
import org.apache.kafka.clients.admin.NewTopic
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, TimeoutException}

class KafkaSpec extends AnyFlatSpec with Matchers {

  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val timer: Timer[IO]               = IO.timer(ExecutionContext.global)

  val fooTopic = "foo"

  def waitFor[A](duration: FiniteDuration)(ioa: => IO[A]): IO[A] =
    IO.race(Timer[IO].sleep(duration), ioa).flatMap {
      case Left(_)  => IO.raiseError(new TimeoutException(duration.toString()))
      case Right(a) => IO.pure(a)
    }

  def DeferredConsumer(maybeRecord: Deferred[IO, ConsumerRecord[IO]]): Consumer[IO] = Consumer.of[IO] {
    case msg => maybeRecord.complete(msg)
  }

  def withEnvironment[A](test: (producer.Producer[IO], Deferred[IO, ConsumerRecord[IO]]) => IO[A]): A = {
    for {
      admin <- KafkaAdminBuilder[IO].resource
      _     <- Resource.make(admin.createTopics(Seq(new NewTopic(fooTopic, 1, 1))))(_ => admin.deleteTopics(Seq(fooTopic)))

      firstRecord <- Resource.liftF(Deferred[IO, ConsumerRecord[IO]])
      _ <- KafkaConsumerBuilder[IO]
        .withTopics(fooTopic)
        .withConsumer(DeferredConsumer(firstRecord))
        .resource

      producer <- KafkaProducerBuilder[IO].resource

    } yield (producer, firstRecord)
  }.use(test.tupled).unsafeRunSync()

  it should "should produce and consume messages" in withEnvironment { (producer, maybeMessage) =>
    for {
      _ <- producer.send(fooTopic, key = 1, value = "bar")
      record <- waitFor(10.seconds) {
        maybeMessage.get
      }
      topic = record.topic
      key   <- record.key[Int]
      value <- record.as[String]
    } yield {
      topic shouldBe "foo"
      key shouldBe 1
      value shouldBe "bar"
    }
  }
}
