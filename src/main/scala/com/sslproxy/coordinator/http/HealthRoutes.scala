package com.sslproxy.coordinator.http

import cats.effect.IO
import cats.syntax.all.*
import com.sslproxy.coordinator.observability.CoordinatorMetrics
import com.sslproxy.coordinator.postgres.PostgresTransactor
import com.sslproxy.coordinator.processor.ProcessorReadiness
import io.circe.Json
import org.http4s.{Header, HttpRoutes, Response, Status}
import org.http4s.circe.*
import org.http4s.dsl.io.*
import org.typelevel.ci.*

import scala.concurrent.duration.*

class HealthRoutes(
  transactor: PostgresTransactor,
  metrics: CoordinatorMetrics,
  processorReadiness: Option[ProcessorReadiness] = None,
  databaseCheckTimeout: FiniteDuration = 5.seconds
):
  private val prometheusContentType =
    Header.Raw(ci"Content-Type", "text/plain; version=0.0.4; charset=utf-8")

  private def readinessResponse: IO[org.http4s.Response[IO]] =
    (
      HealthRoutes.withTimeout(transactor.healthCheck, databaseCheckTimeout),
      HealthRoutes.withTimeout(
        processorReadiness.fold(IO.pure(true))(_.ready),
        databaseCheckTimeout
      )
    ).parTupled.flatMap { case (databaseHealthy, processorsHealthy) =>
      val healthy = databaseHealthy && processorsHealthy
      val status = if healthy then "UP" else "DOWN"
      val json = Json.obj(
        "status" -> Json.fromString(status),
        "components" -> Json.obj(
          "postgres" -> Json.obj("status" -> Json.fromString(if databaseHealthy then "UP" else "DOWN")),
          "processors" -> Json.obj("status" -> Json.fromString(if processorsHealthy then "UP" else "DOWN"))
        )
      )
      if healthy then Ok(json)
      else ServiceUnavailable(json)
    }

  private def metricsResponse: IO[Response[IO]] =
    IO {
      Response[IO](Status.Ok)
        .withEntity(metrics.scrape)
        .withHeaders(prometheusContentType)
    }

  val routes: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root / "live" =>
      Ok(Json.obj("status" -> Json.fromString("UP")))

    case GET -> Root / "ready" => readinessResponse

    case GET -> Root / "actuator" / "health" =>
      readinessResponse

    case GET -> Root / "health" =>
      readinessResponse

    case GET -> Root / "metrics" =>
      metricsResponse

    case GET -> Root / "actuator" / "prometheus" =>
      metricsResponse
  }

object HealthRoutes:
  private[http] def withTimeout(
    healthCheck: IO[Boolean],
    timeout: FiniteDuration
  ): IO[Boolean] =
    healthCheck.timeoutTo(timeout, IO.pure(false)).handleError(_ => false)
