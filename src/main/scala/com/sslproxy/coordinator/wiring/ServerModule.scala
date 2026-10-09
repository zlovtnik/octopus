package com.sslproxy.coordinator.wiring

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.comcast.ip4s.*
import com.sslproxy.coordinator.config.{HttpConfig, PublicStatsConfig}
import com.sslproxy.coordinator.http.{HealthRoutes, PublicStatsRoutes}
import com.sslproxy.coordinator.observability.{CoordinatorMetrics, OperationalStatsService}
import com.sslproxy.coordinator.postgres.PostgresTransactor
import com.sslproxy.coordinator.processor.ProcessorReadiness
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.Server

import scala.concurrent.duration.FiniteDuration

private[coordinator] object ServerModule:
  def resource(
      http: HttpConfig,
      publicStats: PublicStatsConfig,
      transactor: PostgresTransactor,
      metrics: CoordinatorMetrics,
      readiness: ProcessorReadiness,
      statsService: OperationalStatsService,
      databaseCheckTimeout: FiniteDuration
  ): Resource[IO, Server] =
    val healthRoutes = new HealthRoutes(
      transactor, metrics, Some(readiness), databaseCheckTimeout
    )
    val publicStatsRoutes =
      if publicStats.enabled then
        new PublicStatsRoutes(statsService, publicStats.allowedOrigins).routes
      else org.http4s.HttpRoutes.empty[IO]
    val httpPort = Port.fromInt(http.port).getOrElse(
      sys.error(s"Port ${http.port} validated by config but IP4s rejected it")
    )
    EmberServerBuilder.default[IO]
      .withPort(httpPort)
      .withHost(host"0.0.0.0")
      .withHttpApp((healthRoutes.routes <+> publicStatsRoutes).orNotFound)
      .build
