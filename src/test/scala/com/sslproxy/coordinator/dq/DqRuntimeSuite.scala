package com.sslproxy.coordinator.dq

import cats.effect.{IO, Ref}
import munit.CatsEffectSuite

class DqRuntimeSuite extends CatsEffectSuite:
  test("maintenance recovery cannot mask a disconnected report consumer or failed report partition") {
    for
      ready <- Ref.of[IO, Boolean](false)
      health <- DqRuntime.Readiness.create(ready)
      _ <- health.maintenance(true)
      disconnected <- ready.get
      _ = assert(!disconnected)
      _ <- health.reports(true)
      healthy <- ready.get
      _ = assert(healthy)
      _ <- health.partition(0, false)
      _ <- health.partition(1, false)
      _ <- health.maintenance(true)
      _ <- health.partition(0, true)
      failedPartition <- ready.get
      _ = assert(!failedPartition)
      _ <- health.partition(1, true)
      recovered <- ready.get
      _ = assert(recovered)
      _ <- health.reports(false)
      _ <- health.maintenance(true)
      failedConsumer <- ready.get
      _ = assert(!failedConsumer)
      _ <- health.reports(true)
      _ <- health.partition(0, false)
      _ <- health.assignments(Set(1))
      revoked <- ready.get
      _ = assert(revoked)
    yield ()
  }

  test("leases begin one at a time after previous work completes and draining is bounded") {
    for
      events <- Ref.of[IO, Vector[String]](Vector.empty)
      next <- Ref.of[IO, Int](0)
      claim = next.getAndUpdate(_ + 1).flatMap(item => events.update(_ :+ s"claim-$item").as(Some(item)))
      _ <- DqRuntime.drain(2, claim)(item => events.update(_ :+ s"complete-$item"))
      observed <- events.get
      _ = assertEquals(observed, Vector("claim-0", "complete-0", "claim-1", "complete-1"))
    yield ()
  }

  test("an empty queue stops the batch immediately") {
    for
      count <- Ref.of[IO, Int](0)
      _ <- DqRuntime.drain(100, count.update(_ + 1).as(Option.empty[Int]))(_ =>
        IO.raiseError(new IllegalStateException("unexpected_work"))
      )
      observed <- count.get
      _ = assertEquals(observed, 1)
    yield ()
  }

  test("a work failure leaves the next item available for another runtime tick") {
    for
      count <- Ref.of[IO, Int](0)
      result <- DqRuntime
        .drain(100, count.getAndUpdate(_ + 1).map(Some(_)))(_ =>
          IO.raiseError(new IllegalStateException("dependency_unavailable"))
        )
        .attempt
      _ = assert(result.isLeft)
      observed <- count.get
      _ = assertEquals(observed, 1)
    yield ()
  }

  test("stage failures do not skip later runtime stages") {
    for
      events <- Ref.of[IO, Vector[String]](Vector.empty)
      result <- DqRuntime
        .attemptStages(
          List(
            events.update(_ :+ "preparation") *> IO.raiseError(new IllegalStateException("preparation_failed")),
            events.update(_ :+ "output"),
            events.update(_ :+ "outbox") *> IO.raiseError(new IllegalStateException("outbox_failed")),
            events.update(_ :+ "discovery")
          )
        )
        .attempt
      observed <- events.get
      _ = assertEquals(result.left.toOption.map(_.getMessage), Some("preparation_failed"))
      _ = assertEquals(observed, Vector("preparation", "output", "outbox", "discovery"))
    yield ()
  }
