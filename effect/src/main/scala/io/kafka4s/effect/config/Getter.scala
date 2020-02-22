package io.kafka4s.effect.config

import java.util.Properties

import cats.Monad
import cats.implicits._

private[kafka4s] trait Getter[A] {
  def get(properties: Properties, key: String): Getter.Result[A]
}

private[kafka4s] object Getter {
  type Result[A] = Either[GetterException, A]

  def fromEither[A, B](either: => Either[A, B]): Getter[B] =
    (_, key) =>
      either.leftMap {
        case e: Throwable => GetterException(key, e)
        case _            => GetterException(key, new ClassCastException("not a throwable"))
    }

  def emap[A](fn: (Properties, String) => A): Getter[A] =
    (properties, key) => Either.catchNonFatal(fn(properties, key)).leftMap(GetterException(key, _))

  def catchNonFatal[A](f: => A): Getter[A] = Getter.fromEither(Either.catchNonFatal(f))

  implicit def apply[A](implicit getter: Getter[A]): Getter[A] = getter

  implicit val monad = new Monad[Getter] {
    def pure[A](x: A): Getter[A] = (_, _) => Right(x)

    def flatMap[A, B](fa: Getter[A])(f: A => Getter[B]): Getter[B] =
      (p, k) =>
        for {
          a <- fa.get(p, k)
          b <- f(a).get(p, k)
        } yield b

    def tailRecM[A, B](a: A)(f: A => Getter[Either[A, B]]): Getter[B] =
      (p, k) => f(a).get(p, k).flatMap(Getter.fromEither[A, B](_).get(p, k))
  }
}
