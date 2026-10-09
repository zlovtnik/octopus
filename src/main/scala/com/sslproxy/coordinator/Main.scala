package com.sslproxy.coordinator

import cats.effect.{IO, IOApp}
import com.sslproxy.coordinator.config.AppConfig
import com.sslproxy.coordinator.observability.StructuredLogger
import com.sslproxy.coordinator.wiring.{CoordinatorApplication, ObservabilityModule}

object Main extends IOApp.Simple:
  private val log = StructuredLogger(getClass)

  override def run: IO[Unit] =
    val cfg = AppConfig.load

    if !cfg.postgres.enabled then
      log.warn("startup", "status" -> "disabled", "postgres_sink" -> "disabled")
      IO.println(
        "PostgreSQL sink disabled (set POSTGRES_ENABLED=true to enable)"
      ).void
    else
      val appResource = ObservabilityModule.resource.flatMap { metrics =>
        CoordinatorApplication.resource(cfg, metrics)
      }

      log.info(
        "startup",
        "status" -> "starting",
        "postgres_host" -> cfg.postgres.host,
        "postgres_port" -> cfg.postgres.port.toString,
        "postgres_database" -> cfg.postgres.database
      )

      appResource.use(_.joinWithNever)
