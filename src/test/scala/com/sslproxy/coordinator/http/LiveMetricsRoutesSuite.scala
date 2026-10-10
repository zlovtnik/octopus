package com.sslproxy.coordinator.http

import cats.effect.IO
import com.sslproxy.coordinator.observability.CoordinatorMetrics
import io.circe.Json
import munit.CatsEffectSuite
import org.http4s.{Method, Request, Status, Uri}
import org.http4s.circe.*
import org.typelevel.ci.*

import java.time.Instant

class LiveMetricsRoutesSuite extends CatsEffectSuite:
  private val start = Instant.parse("2026-10-08T12:00:00Z")
  private val uri = Uri.unsafeFromString("/internal/metrics/live")

  test("cold telemetry is null and response bypasses caches") {
    val metrics = CoordinatorMetrics.withClock(() => start.toEpochMilli)
    val routes = new LiveMetricsRoutes(metrics, IO.pure(start)).routes.orNotFound
    routes.run(Request[IO](Method.GET, uri)).flatMap { response =>
      response.as[Json].map { json =>
        assertEquals(response.status, Status.Ok)
        assertEquals(json.hcursor.get[String]("asOf"), Right(start.toString))
        assert(json.hcursor.downField("liveStrip").focus.exists(_.isNull))
        assertEquals(response.headers.get(ci"Cache-Control").map(_.head.value), Some("no-store"))
      }
    }
  }

  test("fresh bridge matches coordinator rate and gauge semantics") {
    var millis = start.toEpochMilli
    val metrics = CoordinatorMetrics.withClock(() => millis)
    millis += 301000L
    metrics.recordIngestProcessed(600L)
    metrics.recordPendingLedgerCount(9L)
    metrics.recordBackpressureActive(true)
    metrics.recordIngestInvocation(success = true)
    val at = Instant.ofEpochMilli(millis)
    val routes = new LiveMetricsRoutes(metrics, IO.pure(at)).routes.orNotFound
    routes.run(Request[IO](Method.GET, uri)).flatMap(_.as[Json]).map { json =>
      val live = json.hcursor.downField("liveStrip")
      assertEquals(live.get[Double]("ingestProcessedRatePerSec"), Right(2.0))
      assertEquals(live.get[Long]("pendingLedgerCount"), Right(9L))
      assertEquals(live.get[Boolean]("backpressureActive"), Right(true))
      assertEquals(live.get[String]("lastIngestSuccessAt"), Right(at.toString))
    }
  }

  test("stale or negative gauges cannot appear as measured live data") {
    var millis = start.toEpochMilli
    val metrics = CoordinatorMetrics.withClock(() => millis)
    millis += 301000L
    metrics.recordIngestProcessed(0L)
    metrics.recordPendingLedgerCount(-1L)
    metrics.recordBackpressureActive(false)
    val at = Instant.ofEpochMilli(millis)
    List(at, at.plusSeconds(61)).foldLeft(IO.unit) { (previous, sampledAt) =>
      previous *> new LiveMetricsRoutes(metrics, IO.pure(sampledAt)).routes.orNotFound
        .run(Request[IO](Method.GET, uri)).flatMap(_.as[Json]).map { json =>
          assert(json.hcursor.downField("liveStrip").focus.exists(_.isNull))
        }
    }
  }
