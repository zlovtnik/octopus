package com.sslproxy.coordinator.http

import cats.effect.IO
import com.sslproxy.coordinator.observability.{OperationalStatsService, PublicStats, PublicStatsSource}
import io.circe.Json
import org.http4s.{HttpRoutes, Header, Headers}
import org.http4s.circe.*
import org.http4s.dsl.io.*
import org.typelevel.ci.CIString

import java.time.Instant
import scala.concurrent.duration.*

class PublicStatsRoutes(
  statsService: PublicStatsSource,
  allowedOrigins: List[String]
):
  private def corsHeaders(origin: Option[String]): List[Header.Raw] =
    origin match
      case Some(o) if allowedOrigins.contains(o) =>
        List(
          Header.Raw(CIString("Access-Control-Allow-Origin"), o),
          Header.Raw(CIString("Access-Control-Allow-Methods"), "GET, OPTIONS"),
          Header.Raw(CIString("Access-Control-Allow-Headers"), "Content-Type"),
          Header.Raw(CIString("Vary"), "Origin")
        )
      case _ =>
        List(Header.Raw(CIString("Vary"), "Origin"))

  /** Hard ceiling so a stuck peaks path cannot hang the public socket. */
  private val snapshotTimeout: FiniteDuration = 2.seconds

  private def emptyStats: PublicStats =
    PublicStats(
      asOf = OperationalStatsService.toIso(Instant.now()),
      peaksComputedAt = None,
      peakRecordsDay = None,
      peakRecordsDayDate = None,
      peakRecordsWeek = None,
      peakRecordsWeekStart = None,
      peakRecordsWeekEnd = None,
      liveStrip = None
    )

  /** Response deadline that does not depend on canceling snapshot work.
    *
    * `timeoutTo` on `snapshot` itself can wait out an uncancelable region
    * before returning. Instead the work runs on its own fiber and we race
    * `join` against a sleep: when the deadline wins we answer `emptyStats`
    * immediately. Cleanup is fire-and-forget so a non-cancelable region cannot
    * stall this response.
    */
  private def boundedSnapshot: IO[PublicStats] =
    for
      fiber <- statsService.snapshot.start
      result <- IO.race(fiber.joinWithNever, IO.sleep(snapshotTimeout)).flatMap {
        case Left(stats) => IO.pure(stats)
        case Right(_)    => fiber.cancel.start.void.as(emptyStats)
      }
    yield result

  private def statsJson(s: PublicStats): Json =
    val liveStripJson = s.liveStrip.map { ls =>
      Json.obj(
        "ingestProcessedRatePerSec" -> Json.fromDoubleOrNull(ls.ingestProcessedRatePerSec),
        "pendingLedgerCount" -> Json.fromLong(ls.pendingLedgerCount),
        "lastIngestSuccessAt" -> ls.lastIngestSuccessAt.fold(Json.Null)(Json.fromString),
        "backpressureActive" -> Json.fromBoolean(ls.backpressureActive)
      )
    }.getOrElse(Json.Null)

    Json.obj(
      "asOf" -> Json.fromString(s.asOf),
      "peaksComputedAt" -> s.peaksComputedAt.fold(Json.Null)(Json.fromString),
      "peakRecordsDay" -> s.peakRecordsDay.fold(Json.Null)(Json.fromLong),
      "peakRecordsDayDate" -> s.peakRecordsDayDate.fold(Json.Null)(Json.fromString),
      "peakRecordsWeek" -> s.peakRecordsWeek.fold(Json.Null)(Json.fromLong),
      "peakRecordsWeekStart" -> s.peakRecordsWeekStart.fold(Json.Null)(Json.fromString),
      "peakRecordsWeekEnd" -> s.peakRecordsWeekEnd.fold(Json.Null)(Json.fromString),
      "liveStrip" -> liveStripJson
    )

  val routes: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case request @ GET -> Root / "public" / "stats" =>
      val origin = request.headers.get(CIString("Origin")).map(_.head.value)
      boundedSnapshot
        .flatMap { s =>
          Ok(statsJson(s)).map { resp =>
            val all = corsHeaders(origin) :+
              Header.Raw(CIString("Cache-Control"), "no-store")
            resp.withHeaders(Headers(all.map(h => h: org.http4s.Header.ToRaw)*))
          }
        }.handleErrorWith { _ =>
          ServiceUnavailable(Json.obj("error" -> Json.fromString("Metrics unavailable"))).map(
            _.withHeaders(Headers((corsHeaders(origin) :+ Header.Raw(CIString("Cache-Control"), "no-store")).map(h => h: Header.ToRaw)*))
          )
        }

    case request @ OPTIONS -> Root / "public" / "stats" =>
      val origin = request.headers.get(CIString("Origin")).map(_.head.value)
      NoContent().map { resp =>
        resp.withHeaders(Headers(corsHeaders(origin).map(h => h: org.http4s.Header.ToRaw)*))
      }
  }
