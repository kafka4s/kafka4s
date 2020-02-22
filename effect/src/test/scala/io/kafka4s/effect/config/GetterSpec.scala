package io.kafka4s.effect.config

import java.util.Properties

import cats.implicits._
import io.kafka4s.effect.test.UnitSpec

import scala.concurrent.duration._

class GetterSpec extends UnitSpec {

  val properties = new Properties()
  properties.put("str", "Hello, World!")
  properties.put("int", 12345)
  properties.put("bool", 1)
  properties.put("inf", "Inf")
  properties.put("duration", "1000 ms")

  ".getter[A](key)" should "get and cast a key from the properties object" in {
    properties.getter[String]("str") shouldBe Right("Hello, World!")
    properties.getter[Int]("int") shouldBe Right(12345)
    properties.getter[Boolean]("bool") shouldBe Right(true)
    properties.getter[FiniteDuration]("duration") shouldBe Right(1.second)
  }

  it should "get optional values" in {
    // Some
    properties.getter[Option[String]]("str") shouldBe Right(Some("Hello, World!"))
    properties.getter[Option[Int]]("int") shouldBe Right(Some(12345))
    properties.getter[Option[Boolean]]("bool") shouldBe Right(Some(true))
    // None
    properties.getter[Option[String]]("foo") shouldBe Right(None)
  }

  it should "fail if required key in not in the properties" in {
    val err = properties.getter[String]("foo")
    err.leftMap(_.key) shouldBe Left("foo")
    err.leftMap(_.ex.getMessage) shouldBe Left("no such element")
  }

  it should "fail if there is any exception" in {
    properties.getter[Int]("str") shouldBe a[Left[_, _]]
    properties.getter[FiniteDuration]("int") shouldBe a[Left[_, _]]
    properties.getter[FiniteDuration]("inf") shouldBe a[Left[_, _]]
  }
}
