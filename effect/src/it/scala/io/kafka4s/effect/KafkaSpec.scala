package io.kafka4s.effect

import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{ContextShift, IO, Resource, Timer}
import cats.implicits._
import io.kafka4s.Producer
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

  val foo = "foo"

  def waitFor[A](duration: FiniteDuration)(ioa: => IO[A]): IO[A] =
    IO.race(Timer[IO].sleep(duration), ioa).flatMap {
      case Left(_)  => IO.raiseError(new TimeoutException(duration.toString()))
      case Right(a) => IO.pure(a)
    }

  def waitUntil(duration: FiniteDuration, tryEvery: FiniteDuration = 10.millis)(predicate: IO[Boolean]): IO[Unit] = {
    def loop: IO[Unit] =
      for {
        _  <- Timer[IO].sleep(tryEvery)
        ok <- predicate
        _  <- if (ok) IO.unit else loop
      } yield ()

    for {
      isReady <- loop.start
      _ <- IO.race(Timer[IO].sleep(duration), isReady.join).flatMap {
        case Left(_)  => isReady.cancel >> IO.raiseError(new TimeoutException(duration.toString()))
        case Right(_) => IO.unit
      }
    } yield ()
  }

  def withSingleRecord[A](topic: String)(test: (Producer[IO], Deferred[IO, ConsumerRecord[IO]]) => IO[A]): A = {
    for {
      admin       <- KafkaAdminBuilder[IO].resource
      _           <- Resource.make(admin.createTopics(Seq(new NewTopic(topic, 1, 1))))(_ => admin.deleteTopics(Seq(topic)))
      firstRecord <- Resource.liftF(Deferred[IO, ConsumerRecord[IO]])
      _ <- KafkaConsumerBuilder[IO]
        .withTopics(topic)
        .withConsumer(Consumer.of[IO] {
          case msg => firstRecord.complete(msg)
        })
        .resource

      producer <- KafkaProducerBuilder[IO].resource

    } yield (producer, firstRecord)
  }.use(test.tupled).unsafeRunSync()

  def withMultipleRecords[A](topic: String)(test: (Producer[IO], Ref[IO, List[ConsumerRecord[IO]]]) => IO[A]): A = {
    for {
      admin   <- KafkaAdminBuilder[IO].resource
      _       <- Resource.make(admin.createTopics(Seq(new NewTopic(topic, 1, 1))))(_ => admin.deleteTopics(Seq(topic)))
      records <- Resource.liftF(Ref[IO].of(List.empty[ConsumerRecord[IO]]))
      _ <- KafkaConsumerBuilder[IO]
        .withTopics(foo)
        .withConsumer(Consumer.of[IO] {
          case msg => records.update(_ :+ msg)
        })
        .resource

      producer <- KafkaProducerBuilder[IO].resource

    } yield (producer, records)
  }.use(test.tupled).unsafeRunSync()

  it should "should produce and consume messages" in withSingleRecord(topic = foo) { (producer, maybeMessage) =>
    for {
      _ <- producer.send(foo, key = 1, value = "bar")
      record <- waitFor(10.seconds) {
        maybeMessage.get
      }
      topic = record.topic
      key   <- record.key[Int]
      value <- record.as[String]
    } yield {
      topic shouldBe foo
      key shouldBe 1
      value shouldBe "bar"
    }
  }

  it should "should produce and consume multiple messages" in withMultipleRecords(topic = foo) { (producer, records) =>
    for {
      _ <- (1 to 100).toList.traverse(n => producer.send(foo, value = s"bar #$n"))
      _ <- waitUntil(10.seconds) {
        records.get.map(_.length == 100)
      }
      len    <- records.get.map(_.length)
      record <- records.get.flatMap(l => IO(l.last))
      topic = record.topic
      key   <- record.key[Option[Int]]
      value <- record.as[String]
    } yield {
      len shouldBe 100
      topic shouldBe foo
      key shouldBe None
      value shouldBe "bar #100"
    }
  }
}
