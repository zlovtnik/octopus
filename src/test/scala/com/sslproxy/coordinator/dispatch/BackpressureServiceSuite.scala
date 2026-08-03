package com.sslproxy.coordinator.dispatch

import cats.effect.IO
import com.sslproxy.coordinator.config.BackpressureConfig
import com.sslproxy.coordinator.domain.DatabaseError
import com.sslproxy.coordinator.observability.CoordinatorMetrics
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import munit.CatsEffectSuite

class BackpressureServiceSuite extends CatsEffectSuite:

  private val cfg = BackpressureConfig(
    budgetMultiplier = 4,
    adaptivePullChangeThreshold = 50,
    adaptivePullMinRestartIntervalMs = 10000
  )
  private val ingestBatchSize = 1000
  private val metrics = new CoordinatorMetrics(SimpleMeterRegistry())

  private def service(pending: IO[Either[DatabaseError, Long]]): IO[BackpressureService] =
    BackpressureService.create(cfg, ingestBatchSize, pending, metrics)

  test("budget is ingestBatchSize * multiplier"):
    service(IO.pure(Right(0L))).map(svc => assertEquals(svc.budget, 4000L))

  test("recovery threshold is budget / 2"):
    service(IO.pure(Right(0L))).map(svc => assertEquals(svc.recoveryThreshold, 2000L))

  test("not suspended when pending count is below budget"):
    for
      svc <- service(IO.pure(Right(500L)))
      count <- svc.checkAndAct
      suspended <- svc.isConsumerSuspended
    yield
      assertEquals(count, 500L)
      assertEquals(suspended, false)

  test("suspend when pending count reaches budget"):
    for
      svc <- service(IO.pure(Right(4000L)))
      count <- svc.checkAndAct
      suspended <- svc.isConsumerSuspended
    yield
      assertEquals(count, 4000L)
      assertEquals(suspended, true)

  test("stay suspended when pending count remains above recovery threshold"):
    for
      svc <- service(IO.pure(Right(4000L)))
      _ <- svc.checkAndAct
      _ <- svc.checkAndAct
      suspended <- svc.isConsumerSuspended
    yield
      assertEquals(suspended, true)

  test("resume when pending count falls to recovery threshold after suspension"):
    for
      countRef <- cats.effect.kernel.Ref[IO].of(Right(5000L): Either[DatabaseError, Long])
      svc <- service(countRef.getAndSet(Right(5000L)))
      _ <- svc.checkAndAct
      suspended1 <- svc.isConsumerSuspended
      _ <- countRef.set(Right(1500L))
      count2 <- svc.checkAndAct
      suspended2 <- svc.isConsumerSuspended
    yield
      assertEquals(count2, 1500L)
      assertEquals(suspended1, true)
      assertEquals(suspended2, false)
