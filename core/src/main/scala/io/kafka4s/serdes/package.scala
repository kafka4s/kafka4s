package io.kafka4s

package object serdes extends SerdeImplicits {
  type Result[T] = Either[Throwable, T]
}
