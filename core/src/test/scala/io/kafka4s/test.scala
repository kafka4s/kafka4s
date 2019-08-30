package io.kafka4s

import cats.{ApplicativeError, Id, Monad}
import org.scalatest.{FlatSpec, Matchers}

object test {
  trait UnitSpec extends FlatSpec with Matchers {

    implicit val applicativeError = new Monad[Id] with ApplicativeError[Id, Throwable] {
      
      def flatMap[A, B](fa: Id[A])(f: A => Id[B]): Id[B] = Monad[Id].flatMap(fa)(f(_))

      def tailRecM[A, B](a: A)(f: A => Id[Either[A, B]]): Id[B] = Monad[Id].tailRecM(a)(f(_))

      def raiseError[A](e: Throwable): Id[A] = throw e
      def handleErrorWith[A](fa: Id[A])(f: Throwable => Id[A]): Id[A] = try fa catch {
        case e: Throwable => f(e)
      }
      def pure[A](x: A): Id[A] = x
//      def ap[A, B](ff: Id[A => B])(fa: Id[A]): Id[B] = ff(fa)
    }
  }
}
