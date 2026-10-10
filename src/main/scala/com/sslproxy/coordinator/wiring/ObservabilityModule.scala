package com.sslproxy.coordinator.wiring

import cats.effect.{IO, Resource}
import com.sslproxy.coordinator.observability.{CoordinatorMetrics, CoordinatorTracing}
import io.micrometer.prometheusmetrics.{PrometheusConfig, PrometheusMeterRegistry}

private[coordinator] object ObservabilityModule:
  val resource: Resource[IO, CoordinatorMetrics] =
    for
      registry <- Resource.make(IO(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT)))(value => IO(value.close()))
      metrics <- Resource.eval(IO(new CoordinatorMetrics(registry)))
      _ <- metrics.jvmMetrics
    yield metrics

  // Application acquisition starts with tracing inside the JVM meter lifetime.
  val tracingResource: Resource[IO, Unit] = CoordinatorTracing.resource
