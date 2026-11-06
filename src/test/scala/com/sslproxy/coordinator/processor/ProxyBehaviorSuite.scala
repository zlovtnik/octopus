package com.sslproxy.coordinator.processor

import io.circe.Json
import io.circe.syntax.*
import java.time.{Duration, Instant}
import munit.FunSuite

class ProxyBehaviorSuite extends FunSuite:
  private val openedAt = Instant.parse("2026-11-01T12:59:58Z")
  private val closedAt = Instant.parse("2026-11-01T13:00:02Z")
  private val correlation = "00000000-0000-4000-8000-000000000100"
  private val policy = ProxyBehavior.Policy("deployment-a", "reviewed-v1", Duration.ofSeconds(30), 3600,
    Instant.parse("2026-11-01T11:00:00Z"), Instant.parse("2026-11-01T15:00:00Z"), true, true,
    Instant.parse("2026-11-01T14:00:00Z"), Instant.parse("2026-11-01T12:00:00Z"), 100, 20)

  private def event(id: Int, eventType: String, at: Instant, sequence: Int): Json = Json.obj(
    "event_id" -> f"00000000-0000-4000-8000-$id%012d".asJson,
    "correlation_id" -> correlation.asJson, "type" -> eventType.asJson, "time" -> at.toString.asJson,
    "event_sequence" -> sequence.asJson, "blocked" -> false.asJson,
    "host" -> "Example.COM:443".asJson, "authority_raw" -> "Example.COM:443".asJson,
    "authority_source" -> "connect_authority".asJson, "evidence_version" -> ProxyEvidence.Version.asJson,
    "bytes_up" -> 17.asJson, "bytes_down" -> 29.asJson, "duration_ms" -> 4200.asJson,
    "measurement" -> Json.obj("version" -> "proxy-measurement-v1".asJson,
      "bytes_status" -> "measured_terminal".asJson, "source" -> "successful_socket_writes".asJson,
      "byte_unit" -> "bytes".asJson, "up_direction" -> "client_to_destination".asJson,
      "down_direction" -> "destination_to_client".asJson, "termination" -> "measured_terminal".asJson,
      "duration_status" -> "connection_elapsed".asJson))
  private val open = ProxyBehavior.Source(event(1, "tunnel_open", openedAt, 1), 1L)
  private val close = ProxyBehavior.Source(event(2, "tunnel_close", closedAt, 2), 2L)
  private def build(sources: ProxyBehavior.Source*): ProxyBehavior.Result = ProxyBehavior.build(policy, sources.toList).toOption.get

  test("one validated terminal measurement is counted in exactly its terminal event-time window"):
    val result = build(close, open, close)
    assertEquals(result.flows.size, 1)
    val flow = result.flows.head
    assertEquals(flow.disposition, "validated")
    assertEquals(flow.evidence.hcursor.get[Long]("bytes_up"), Right(17L))
    assertEquals(flow.evidence.hcursor.get[Long]("observation_elapsed_ms"), Right(4000L))
    assertEquals(flow.evidence.hcursor.get[Long]("duration_ms"), Right(4200L))
    assertEquals(result.windows.map(_.evidence.hcursor.get[Int]("validated_flows").toOption.get), List(0, 1))
    assertEquals(result.windows.map(_.evidence.hcursor.get[Int]("allowed_observations").toOption.get), List(1, 1))
    assert(!flow.semanticText.contains(correlation))
    assertEquals(flow.semanticText, "proxy flow example.com validated transfer measured_terminal duration scope retained separately")

  test("replay and ingest order preserve all fingerprints and calculated-time refresh preserves stable content"):
    val first = build(open, close)
    assertEquals(first, build(close, open, close, open))
    val refreshed = ProxyBehavior.build(policy.copy(asOf = policy.asOf.plusSeconds(1)), List(open, close)).toOption.get
    assertEquals(first.flows.map(_.fingerprint), refreshed.flows.map(_.fingerprint))
    assert(first.flows.head.evidence != refreshed.flows.head.evidence)
    val advancingCoverage = ProxyBehavior.build(policy.copy(coverageEnd = policy.asOf.plusSeconds(1)), List(open, close)).toOption.get
    assertEquals(first.windows.map(_.fingerprint), advancingCoverage.windows.map(_.fingerprint))

  test("legacy and partial terminal counters never become measured flow totals"):
    val legacy = close.copy(row = close.row.mapObject(_.remove("measurement")))
    val unknown = build(open, legacy).flows.head.evidence
    assertEquals(unknown.hcursor.get[String]("bytes_status"), Right("legacy_unknown"))
    assert(unknown.hcursor.downField("bytes_up").focus.get.isNull)
    val partial = close.copy(row = close.row.deepMerge(Json.obj("measurement" -> Json.obj(
      "bytes_status" -> "partial".asJson, "termination" -> "partial".asJson))))
    val result = build(open, partial)
    assertEquals(result.flows.head.evidence.hcursor.get[Long]("partial_bytes_up"), Right(17L))
    assert(result.flows.head.evidence.hcursor.downField("bytes_up").focus.get.isNull)
    assertEquals(result.windows.last.evidence.hcursor.get[Int]("measured_flows"), Right(0))
    assert(result.windows.last.evidence.hcursor.downField("bytes_up").focus.get.isNull)
    assertEquals(result.windows.last.evidence.hcursor.get[String]("transfer_totals_status"), Right("unavailable"))

  test("provenance units directions and termination are required even for declared terminal bytes"):
    List("source", "byte_unit", "up_direction", "down_direction", "termination").foreach { field =>
      val terminal = close.copy(row = close.row.mapObject(_.add("measurement", close.row.hcursor.downField("measurement").focus.get.mapObject(_.remove(field)))))
      assert(build(open, terminal).flows.head.evidence.hcursor.downField("bytes_up").focus.get.isNull)
    }

  test("late terminal arrival changes a missing-side disposition and distinct terminal IDs invalidate it"):
    assertEquals(build(open).flows.head.disposition, "missing_terminal")
    assertEquals(build(open, close).flows.head.disposition, "validated")
    val duplicateRole = close.copy(row = event(3, "tunnel_close", closedAt, 2), revision = 3L)
    assertEquals(build(open, close, duplicateRole).flows.head.disposition, "ambiguous")
    assertEquals(build(open, close, duplicateRole).windows.map(_.evidence.hcursor.get[Int]("validated_flows").toOption.get).sum, 0)

  test("one raw host cannot hide conflicting normalized endpoint ports"):
    val differentPort = close.copy(row = close.row.deepMerge(Json.obj("authority_raw" -> "example.com:8443".asJson)))
    assertEquals(build(open, differentPort).flows.head.disposition, "inconsistent")
    assertEquals(build(open, differentPort).windows.map(_.evidence.hcursor.get[Int]("validated_flows").toOption.get).sum, 0)

  test("global trusted conflicts invalidate a correlation even when the marker row is outside the event slice"):
    val marker = ProxyBehavior.Source(Json.obj("event_id" -> "00000000-0000-4000-8000-000000000400".asJson), 4L,
      Json.obj(("a" * 64) -> correlation.asJson))
    assertEquals(build(open, close, marker).flows.head.disposition, "ambiguous")
    assertEquals(build(open, close, marker).windows.last.sourceWatermark, 4L)
    val spoofed = close.copy(row = close.row.deepMerge(Json.obj("source_conflicts" -> marker.conflicts)))
    assertEquals(build(open, spoofed).flows.head.disposition, "validated")

  test("coverage requires a strict upper bound beyond the inclusive pairing endpoint"):
    val end = openedAt.plus(policy.pairingHorizon)
    val limited = policy.copy(coverageEnd = end)
    assertEquals(ProxyBehavior.build(limited, List(open, close)).toOption.get.flows.head.disposition, "coverage_limited")
    val unattested = policy.copy(conflictsAttested = false)
    assertEquals(ProxyBehavior.build(unattested, List(open, close)).toOption.get.flows.head.disposition, "conflict_coverage_unavailable")

  test("HTTP observations have no invented flow and blocked observations use observed row counts"):
    val http = close.copy(row = event(10, "http_proxied", closedAt, 0), revision = 10)
    val block = close.copy(row = event(11, "http_blocked", closedAt, 0).deepMerge(Json.obj("blocked" -> true.asJson)), revision = 11)
    val result = build(http, block)
    assertEquals(result.flows, Nil)
    assertEquals(result.windows.last.evidence.hcursor.get[Int]("allowed_observations"), Right(1))
    assertEquals(result.windows.last.evidence.hcursor.get[Int]("blocked_observations"), Right(1))
    assertEquals(result.windows.last.evidence.hcursor.get[String]("last_event_id"), Right("00000000-0000-4000-8000-000000000011"))

  test("sanitized grouping and explicit source/output budgets fail closed"):
    val sanitized = close.copy(row = close.row.deepMerge(Json.obj("sanitization_status" -> "sanitized".asJson)))
    assertEquals(build(sanitized).excludedObservations, 1)
    assert(ProxyBehavior.build(policy.copy(maxSourceRows = 1), List(open, close)).isLeft)
    assert(ProxyBehavior.build(policy.copy(maxWindows = 1), List(open, close)).isLeft)
    assert(ProxyBehavior.build(policy.copy(pairingHorizon = Duration.ZERO), List(open, close)).isLeft)
    assert(ProxyBehavior.build(policy.copy(windowSeconds = 7), List(open, close)).isLeft)
