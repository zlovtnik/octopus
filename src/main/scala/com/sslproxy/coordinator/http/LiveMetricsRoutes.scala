package com.sslproxy.coordinator.http

import cats.effect.IO
import com.sslproxy.coordinator.observability.CoordinatorMetrics
import io.circe.Json
import org.http4s.{Header, HttpRoutes}
import org.http4s.circe.*
import org.http4s.dsl.io.*
import org.typelevel.ci.*

import java.time.Instant

/** Internal read-only telemetry bridge for the dedicated C++ materializer.
  * Reads in-process measurements only; never queries PostgreSQL or stores.
  */
final class LiveMetricsRoutes(metrics: CoordinatorMetrics, now: IO[Instant] = IO.realTimeInstant):
  val routes: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root / "internal" / "metrics" / "live" =>
      now.flatMap { at =>
        val rate = metrics.brokerProcessedRatePerSec(at.toEpochMilli)
        val pending = metrics.pendingLedgerCountValue
        val live =
          if metrics.publicReadingsFresh(at.toEpochMilli) && rate.isFinite && rate >= 0 && pending >= 0 then
            Json.obj(
              "ingestProcessedRatePerSec" -> Json.fromDoubleOrNull(rate),
              "pendingLedgerCount" -> Json.fromLong(pending),
              "brokerLagCount" -> metrics.brokerLagCountValue(at.toEpochMilli).fold(Json.Null)(Json.fromLong),
              "lastIngestSuccessAt" -> metrics.ingestLastSuccessEpochSeconds
                .fold(Json.Null)(ts => Json.fromString(Instant.ofEpochSecond(ts).toString)),
              "backpressureActive" -> Json.fromBoolean(metrics.backpressureActiveValue)
            )
          else Json.Null
        Ok(Json.obj("asOf" -> Json.fromString(at.toString), "liveStrip" -> live))
          .map(_.putHeaders(Header.Raw(ci"Cache-Control", "no-store")))
      }
  }
