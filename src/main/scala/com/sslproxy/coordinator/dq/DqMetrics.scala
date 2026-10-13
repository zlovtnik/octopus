package com.sslproxy.coordinator.dq

import cats.effect.IO
import cats.effect.std.Semaphore
import com.sslproxy.coordinator.observability.{CoordinatorMetrics, StructuredLogger}
import com.sslproxy.coordinator.postgres.sql.DqMetricsSql
import doobie.implicits.*

object DqMetrics:
  private val log = StructuredLogger(getClass)
  def sample(store: DqStore, metrics: CoordinatorMetrics, semaphore: Semaphore[IO]): IO[Unit] =
    semaphore.permit.use(_ => DqMetricsSql.sample(store.settings.installationId,store.settings.namespace).transact(store.xa))
      .flatMap(values => IO(metrics.recordDq(values)))
      .handleErrorWith(_ => IO(log.warn("dq_metrics", "status" -> "sample_unavailable")))
