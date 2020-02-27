package io.kafka4s.effect

import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{Clock, ContextShift, IO, Resource, Timer}
import cats.implicits._
import io.kafka4s._
import io.kafka4s.dsl._
import io.kafka4s.consumer._
import io.kafka4s.effect.admin.KafkaAdminBuilder
import io.kafka4s.effect.consumer.KafkaConsumerBuilder
import io.kafka4s.effect.producer.KafkaProducerBuilder
import org.apache.kafka.clients.admin.NewTopic
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, TimeoutException}

class KafkaSpec extends AnyFlatSpec with Matchers { self =>

  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val timer: Timer[IO]               = IO.timer(ExecutionContext.global)

  val foo  = "foo"
  val boom = "boom"

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
        case Left(_)  => isReady.cancel.start >> IO.raiseError(new TimeoutException(duration.toString()))
        case Right(_) => IO.unit
      }
    } yield ()
  }

  def executionTime: Resource[IO, Long] =
    Resource.make(Clock[IO].monotonic(MILLISECONDS))(t0 =>
      for {
        t1 <- Clock[IO].monotonic(MILLISECONDS)
        t = FiniteDuration(t1 - t0, SECONDS)
        _ <- IO(println(s"Test completed in $t"))
      } yield ())

  def prepareTopics(topics: Seq[String]): Resource[IO, Unit] =
    for {
      admin <- KafkaAdminBuilder[IO].resource
      newTopics = topics.map(new NewTopic(_, 1, 1))
      _ <- Resource.make(admin.createTopics(newTopics))(_ => admin.deleteTopics(topics))
    } yield ()

  def withSingleRecord[A](topics: String*)(test: (Producer[IO], Deferred[IO, ConsumerRecord[IO]]) => IO[A]): A = {
    for {
      _           <- executionTime
      _           <- prepareTopics(topics)
      firstRecord <- Resource.liftF(Deferred[IO, ConsumerRecord[IO]])
      _ <- KafkaConsumerBuilder[IO]
        .withTopics(topics: _*)
        .withConsumer(Consumer.of[IO] {
          case Topic("boom") => IO.raiseError(new Exception("Somebody set up us the bomb"))
          case msg           => firstRecord.complete(msg)
        })
        .resource

      producer <- KafkaProducerBuilder[IO].resource

    } yield (producer, firstRecord)
  }.use(test.tupled).unsafeRunSync()

  def withMultipleRecords[A](topics: String*)(test: (Producer[IO], Ref[IO, List[ConsumerRecord[IO]]]) => IO[A]): A = {
    for {
      _       <- executionTime
      _       <- prepareTopics(topics)
      records <- Resource.liftF(Ref[IO].of(List.empty[ConsumerRecord[IO]]))
      _ <- KafkaConsumerBuilder[IO]
        .withTopics(topics.toSet)
        .withConsumer(Consumer.of[IO] {
          case Topic("boom") => IO.raiseError(new Exception("Somebody set up us the bomb"))
          case msg           => records.update(_ :+ msg)
        })
        .resource

      producer <- KafkaProducerBuilder[IO].resource

    } yield (producer, records)
  }.use(test.tupled).unsafeRunSync()

  it should "should produce and consume messages" in withSingleRecord(topics = foo) { (producer, maybeMessage) =>
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

  it should "should produce and consume multiple messages" in withMultipleRecords(topics = foo) { (producer, records) =>
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

  it should "should not stop consume even if there is an exception in the consumer" in withMultipleRecords(topics = foo,
                                                                                                           boom) {
    (producer, records) =>
      for {
        _ <- (1 to 50).toList.traverse(n => producer.send(foo, value = s"bar #$n"))
        _ <- Timer[IO].sleep(5.millis)
        _ <- producer.send(boom, value = "All your base are belong to us.")
        _ <- Timer[IO].sleep(5.millis)
        _ <- (51 to 100).toList.traverse(n => producer.send(foo, value = s"bar #$n"))
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
