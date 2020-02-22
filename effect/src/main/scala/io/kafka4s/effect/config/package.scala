package io.kafka4s.effect

import java.util.Properties

import cats.implicits._
import com.typesafe.config.ConfigFactory

import scala.collection.JavaConverters._

package object config extends GetterImplicits {

  private[kafka4s] def mapToProperties(map: Map[String, String]): Properties = {
    map.foldLeft(new Properties()) {
      case (props, item) =>
        val (key, value) = item
        props.put(key, value)
        props
    }
  }

  private[kafka4s] def configToProperties(path: String): Either[Throwable, Properties] =
    for {
      configObj <- Either.catchNonFatal {
        ConfigFactory.load().getObject(path).unwrapped().asScala.toMap
      }
    } yield
      configObj.foldLeft[Properties](new Properties()) {
        case (props, item) =>
          val (key, value)  = item
          val normalizedKey = key.replaceAll(raw"-", raw".")
          props.put(normalizedKey, value)
          props
      }

  implicit class PropertiesOps(val properties: Properties) extends AnyVal {
    def getter[T](key: String)(implicit G: Getter[T]): Getter.Result[T] = G.get(properties, key)
  }
}
