package io.kafka4s.effect.config

import cats.implicits._

import scala.concurrent.duration._

private[kafka4s] trait GetterImplicits {

  private val anyRefGetter: Getter[AnyRef] = (properties, key) =>
    if (properties.containsKey(key)) Right(properties.get(key))
    else Left(GetterException(key, new NoSuchElementException("no such element")))

  implicit val stringGetter: Getter[String] =
    anyRefGetter.flatMap(a => Getter.catchNonFatal(a.toString))

  implicit val intGetter: Getter[Int] =
    stringGetter.flatMap(a => Getter.catchNonFatal(a.toInt))

  implicit val booleanGetter: Getter[Boolean] =
    stringGetter.map(_.toLowerCase()).map { value =>
      value == "true" || value == "1" || value == "y"
    }

  implicit val finiteDurationGetter: Getter[FiniteDuration] = for {
    v <- stringGetter
    d <- Getter.catchNonFatal(Duration.create(v))
    f <- Getter.fromEither {
      d match {
        case d: FiniteDuration => Right(d)
        case d                 => Left(new NumberFormatException(s""""${d.toString}" is not a finite duration"""))
      }
    }
  } yield f

  implicit def optionalGetter[T](implicit getter: Getter[T]): Getter[Option[T]] =
    (properties, key) => if (properties.containsKey(key)) getter.get(properties, key).map(Some(_)) else Right(None)
}
