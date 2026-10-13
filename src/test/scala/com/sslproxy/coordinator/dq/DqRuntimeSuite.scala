package com.sslproxy.coordinator.dq

import cats.effect.{IO, Ref}
import munit.CatsEffectSuite

class DqRuntimeSuite extends CatsEffectSuite:
  test("leases begin one at a time after previous work completes and draining is bounded") {
    for
      events <- Ref.of[IO,Vector[String]](Vector.empty)
      next <- Ref.of[IO,Int](0)
      claim = next.getAndUpdate(_+1).flatMap(item => events.update(_ :+ s"claim-$item").as(Some(item)))
      _ <- DqRuntime.drain(2,claim)(item => events.update(_ :+ s"complete-$item"))
      observed <- events.get
      _ = assertEquals(observed,Vector("claim-0","complete-0","claim-1","complete-1"))
    yield ()
  }

  test("an empty queue stops the batch immediately") {
    for
      count <- Ref.of[IO,Int](0)
      _ <- DqRuntime.drain(100,count.update(_+1).as(Option.empty[Int]))(_ => IO.raiseError(new IllegalStateException("unexpected_work")))
      observed <- count.get
      _ = assertEquals(observed,1)
    yield ()
  }

  test("a work failure leaves the next item available for another runtime tick") {
    for
      count <- Ref.of[IO,Int](0)
      result <- DqRuntime.drain(100,count.getAndUpdate(_+1).map(Some(_)))(_ => IO.raiseError(new IllegalStateException("dependency_unavailable"))).attempt
      _ = assert(result.isLeft)
      observed <- count.get
      _ = assertEquals(observed,1)
    yield ()
  }
