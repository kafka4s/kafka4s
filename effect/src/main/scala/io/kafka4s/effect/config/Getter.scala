package io.kafka4s.effect.config

import java.util.Properties

import cats.implicits._

private[kafka4s] trait Getter[T] {
  def get(properties: Properties, key: String): Getter.Result[T]
}

private[kafka4s] object Getter {
  type Result[T] = Either[Throwable, T]

  def emap[T](fn: (Properties, String) => T): Getter[T] =
    (properties, key) => Either.catchNonFatal(fn(properties, key)).leftMap(ex => GetterException(key, ex))

  implicit def apply[T](implicit getter: Getter[T]): Getter[T] = getter
}
