package com.sslproxy.coordinator.http

import cats.effect.IO
import munit.CatsEffectSuite

import scala.concurrent.duration.*

class HealthRoutesSuite extends CatsEffectSuite:
  test("stalled database health checks time out as unhealthy"):
    HealthRoutes.withTimeout(IO.never, 5.millis).map { healthy =>
      assertEquals(healthy, false)
    }
