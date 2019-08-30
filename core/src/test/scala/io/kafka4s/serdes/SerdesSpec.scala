package io.kafka4s.serdes

import java.util.UUID

import io.kafka4s.test.UnitSpec

class SerdesSpec extends UnitSpec {

  def test[T](input: T)(implicit serde: Serde[T]): Either[Throwable, T] = for {
    in <- serde.serialize(input)
    out <- serde.deserialize(in)
  } yield out

  it should "should encode/decode a Float" in {
    test[Float](1.0f) shouldBe Right(1.0f)
  }

  it should "should encode/decode an Int" in {
    test[Int](1) shouldBe Right(1)
  }

  it should "should encode/decode a Double" in {
    test[Double](1.0f) shouldBe Right(1.0f)
  }

  it should "should encode/decode a Long" in {
    test[Long](1L) shouldBe Right(1L)
  }

  it should "should encode/decode a Short" in {
    test[Short](1.toShort) shouldBe Right(1.toShort)
  }

  it should "should encode/decode a String" in {
    test[String]("Hello, World!") shouldBe Right("Hello, World!")
    test[String]("Mátyás") shouldBe Right("Mátyás")
    test[String]("小笼包") shouldBe Right("小笼包")
  }

  it should "should encode/decode an UUID" in {
    val uuid = UUID.randomUUID()
    test[UUID](uuid) shouldBe Right(uuid)
  }
}
