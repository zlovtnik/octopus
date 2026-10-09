package com.sslproxy.coordinator.wiring

import cats.effect.{IO, Resource}
import com.sslproxy.coordinator.observability.{CoordinatorMetrics, CoordinatorTracing}
import io.micrometer.core.instrument.simple.SimpleMeterRegistry

private[coordinator] object ObservabilityModule:
  val resource: Resource[IO, CoordinatorMetrics] =
    for
      // jvmMetrics also closes the registry; this idempotent fallback covers
      // failure before the JVM meter resource has been acquired.
      registry <- Resource.make(IO(new SimpleMeterRegistry()))(value => IO(value.close()))
      metrics <- Resource.eval(IO(new CoordinatorMetrics(registry)))
      _ <- metrics.jvmMetrics
    yield metrics

  // Application acquisition starts with tracing inside the JVM meter lifetime.
  val tracingResource: Resource[IO, Unit] = CoordinatorTracing.resource
