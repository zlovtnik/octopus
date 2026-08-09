package com.sslproxy.coordinator.http

import cats.effect.IO
import munit.CatsEffectSuite

import scala.concurrent.duration.*

class HealthRoutesSuite extends CatsEffectSuite:
  test("completed database health checks remain healthy"):
    HealthRoutes.withTimeout(IO.pure(true), 5.millis).map { healthy =>
      assertEquals(healthy, true)
    }

  test("stalled database health checks time out as unhealthy"):
    HealthRoutes.withTimeout(IO.never, 5.millis).map { healthy =>
      assertEquals(healthy, false)
    }

  test("raised database health checks are reported as unhealthy"):
    HealthRoutes.withTimeout(IO.raiseError(RuntimeException("database unavailable")), 5.millis).map { healthy =>
      assertEquals(healthy, false)
    }
