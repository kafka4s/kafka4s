package io.kafka4s.effect.config

import scala.concurrent.duration._

private[kafka4s] trait GetterImplicits {

  implicit val stringGetter: Getter[String] = Getter.emap { (properties, key) =>
    properties.get(key).toString
  }

  implicit val finiteDurationGetter: Getter[FiniteDuration] =
    Getter.emap { (properties, key) =>
      Duration.create(properties.get(key).toString) match {
        case d: FiniteDuration => d
        case d                 => throw new NumberFormatException(s""""${d.toString}" is not a finite duration""")
      }
    }

  implicit def optionalGetter[T](implicit getter: Getter[T]): Getter[Option[T]] =
    (properties, key) => if (properties.containsKey(key)) getter.get(properties, key).map(Some(_)) else Right(None)
}
