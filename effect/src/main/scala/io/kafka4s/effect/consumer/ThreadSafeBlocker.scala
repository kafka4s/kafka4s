package io.kafka4s.effect.consumer

import cats.effect.concurrent.Semaphore
import cats.effect.{Blocker, Concurrent, ContextShift, Resource}

import scala.concurrent.blocking

class ThreadSafeBlocker[F[_]] private (blocker: Blocker, semaphore: Semaphore[F])(implicit F: Concurrent[F],
                                                                                  CS: ContextShift[F]) {

  def delay[A](thunk: => A): F[A] = semaphore.withPermit(blocker.delay(blocking(thunk)))
}

object ThreadSafeBlocker {

  def resource[F[_]](blocker: Blocker, maxConcurrency: Int = 1)(
    implicit F: Concurrent[F],
    CS: ContextShift[F]): Resource[F, ThreadSafeBlocker[F]] =
    for {
      semaphore <- Resource.liftF(Semaphore[F](maxConcurrency))
    } yield new ThreadSafeBlocker(blocker, semaphore)
}
