package com.sslproxy.coordinator.processor

import io.circe.Json
import io.circe.syntax.*
import java.time.Instant
import munit.FunSuite

class ProxyBehaviorStatisticsSuite extends FunSuite:
  private val start = Instant.parse("2026-11-01T12:00:00Z")
  private val end = start.plusSeconds(3600)
  private def observation(index: Int, seconds: Long, kind: String = "http_proxied", status: String = "not_applied"): Json =
    Json.obj("event_id" -> s"event-$index".asJson, "time" -> start.plusSeconds(seconds).toString.asJson,
      "type" -> kind.asJson, "coalescing" -> Json.obj("status" -> status.asJson))
  private def flow(key: String, at: Instant, up: Long = 100, down: Long = 200,
      status: String = "measured_terminal", disposition: String = "validated"): ProxyBehavior.Projection =
    ProxyBehavior.Projection(key, Json.obj(
      "opened_at" -> at.minusSeconds(1).toString.asJson,
      "bytes_up" -> up.asJson, "bytes_down" -> down.asJson, "bytes_status" -> status.asJson,
      "duration_ms" -> 1000.asJson, "duration_status" -> "connection_elapsed".asJson),
      "fixture", "fixture", disposition, 1L, at)
  private def polling(rows: List[Json], flows: List[ProxyBehavior.Projection] = Nil, complete: Boolean = true): Json =
    ProxyBehaviorStatistics.polling(rows, flows, start, end, complete, complete)
  private val regular = (0 until 6).map(index => observation(index, index * 600L)).toList
  private val history = (1 to 6).map(index => flow(s"prior-$index", start.minusSeconds(index * 600L))).toList

  test("ordinary regular telemetry and update observations are periodic candidates only"):
    List("telemetry", "software_update").foreach { label =>
      val rows = regular.map(_.deepMerge(Json.obj("reason" -> label.asJson)))
      val result = polling(rows)
      assertEquals(result.hcursor.get[Boolean]("candidate"), Right(true))
      assertEquals(result.hcursor.get[String]("interpretation"), Right("periodicity_candidate_only"))
      assertEquals(result.hcursor.get[String]("calibration_status"), Right("provisional_unvalidated"))
      assertEquals(result.hcursor.downField("observations").get[Int]("sample_size"), Right(6))
      assertEquals(result.hcursor.downField("observations").get[BigDecimal]("median_interarrival_ms"), Right(BigDecimal(600000)))
      assertEquals(result.hcursor.downField("observations").get[BigDecimal]("relative_mad"), Right(BigDecimal(0)))
      assertEquals(result.hcursor.downField("observations").get[Int]("active_bin_count"), Right(6))
      assert(!result.noSpaces.contains("confirmed_threat"))
    }

  test("irregular controls do not become candidates and samples use unique times"):
    val times = List(0L, 10L, 300L, 900L, 1800L, 3500L)
    val irregular = times.zipWithIndex.map((seconds, index) => observation(index, seconds))
    val result = polling(irregular)
    assertEquals(result.hcursor.get[Boolean]("candidate"), Right(false))
    assertEquals(result.hcursor.downField("observations").get[String]("disposition"), Right("irregular"))
    assertEquals(polling(regular), polling(regular.reverse ++ regular))
    val collisions = (0 until 20).map(index => observation(index, 600L)).toList
    val small = polling(collisions)
    assertEquals(small.hcursor.downField("observations").get[Int]("sample_size"), Right(1))
    assertEquals(small.hcursor.downField("observations").get[String]("disposition"), Right("insufficient_samples"))

  test("flow starts do not count open and close as two polling observations"):
    val flows = (0 until 6).map(index => flow(s"flow-$index", start.plusSeconds(index * 600L + 1))).toList
    val lifecycle = regular.flatMap(row => List(row.deepMerge(Json.obj("type" -> "tunnel_open".asJson)),
      row.deepMerge(Json.obj("type" -> "tunnel_close".asJson))))
    val result = polling(lifecycle, flows ++ flows)
    assertEquals(result.hcursor.downField("observations").get[Int]("sample_size"), Right(0))
    assertEquals(result.hcursor.downField("validated_flow_starts").get[Int]("sample_size"), Right(6))
    assertEquals(result.hcursor.get[Boolean]("candidate"), Right(true))
    assertEquals(polling(lifecycle, flows), polling(lifecycle.reverse, flows.reverse))

  test("flow-start samples and references use the same half-open event-time window"):
    val atStart = flow("at-start", start.plusSeconds(1))
    val beforeStart = flow("before-start", start)
    val atEnd = flow("at-end", end.plusSeconds(1))
    val crossesEnd = flow("crosses-end", end.plusSeconds(10)).copy(evidence =
      flow("crosses-end", end.plusSeconds(10)).evidence.deepMerge(Json.obj("opened_at" -> end.minusSeconds(10).toString.asJson)))
    val unavailable = flow("missing-open", start.plusSeconds(10), disposition = "missing_open")
    val malformed = flow("malformed", start.plusSeconds(10)).copy(evidence =
      flow("malformed", start.plusSeconds(10)).evidence.deepMerge(Json.obj("opened_at" -> "invalid".asJson)))
    val result = polling(Nil, List(atStart, beforeStart, atEnd, crossesEnd, unavailable, malformed))
    assertEquals(result.hcursor.get[List[String]]("flow_keys"), Right(List("at-start", "crosses-end")))
    assertEquals(result.hcursor.downField("validated_flow_starts").get[Int]("sample_size"), Right(2))
    assertEquals(result.hcursor.downField("validated_flow_starts").get[List[String]]("unique_times"),
      Right(List(start.toString, end.minusSeconds(10).toString)))

  test("collection and coalescing censor candidates without inventing attempts"):
    val coalesced = regular.map(_.deepMerge(Json.obj("coalescing" -> Json.obj(
      "status" -> "emitted".asJson, "suppressed_previous_window" -> 2.asJson))))
    val result = polling(coalesced)
    assertEquals(result.hcursor.get[Boolean]("candidate"), Right(false))
    assertEquals(result.hcursor.downField("observations").get[Boolean]("periodic_pattern"), Right(true))
    assertEquals(result.hcursor.downField("observations").get[String]("disposition"), Right("coalescing_censored"))
    assertEquals(result.hcursor.get[BigInt]("suppressed_previous_window"), Right(BigInt(12)))
    assertEquals(polling(regular, complete = false).hcursor.downField("observations").get[String]("disposition"), Right("collection_censored"))
    assertEquals(polling(regular.take(5)).hcursor.downField("observations").get[String]("disposition"), Right("insufficient_samples"))

  test("sparse active bins and malformed intervals cannot produce candidates"):
    val shortBurst = (0 until 6).map(index => observation(index, index.toLong)).toList
    assertEquals(polling(shortBurst).hcursor.downField("observations").get[String]("disposition"), Right("insufficient_active_bins"))
    val malformed = regular.map(_.deepMerge(Json.obj("time" -> "invalid".asJson)))
    assertEquals(polling(malformed).hcursor.get[Boolean]("candidate"), Right(false))
    assertEquals(ProxyBehaviorStatistics.polling(regular, Nil, end, start, true, true)
      .hcursor.downField("observations").get[String]("disposition"), Right("invalid_window"))

  test("transfer history is strictly earlier, scoped by caller and replay stable"):
    val current = flow("current", start.plusSeconds(10), 1000, 200)
    val equal = flow("equal", current.observedAt, 999999)
    val future = flow("future", end, 999999)
    val result = ProxyBehaviorStatistics.transfer(List(current), history ++ List(current, equal, future), Some(true))
    assertEquals(result.hcursor.get[Int]("baseline_sample_size"), Right(6))
    assertEquals(result.hcursor.get[List[String]]("baseline_flow_keys"), Right(history.map(_.key).sorted))
    assertEquals(result.hcursor.downField("upload_profile").get[BigDecimal]("historical_median"), Right(BigDecimal(100)))
    assertEquals(result.hcursor.downField("upload_profile").get[BigDecimal]("maximum_upward_robust_deviation"), Right(BigDecimal(90)))
    assertEquals(result.hcursor.get[Boolean]("candidate"), Right(true))
    assertEquals(result.hcursor.get[String]("interpretation"), Right("transfer_anomaly_candidate_only"))
    assertEquals(result.hcursor.get[Boolean]("new_to_retained_scope"), Right(true))
    assertEquals(result, ProxyBehaviorStatistics.transfer(List(current, current), (history ++ List(current, equal, future)).reverse ++ history, Some(true)))

  test("partial unavailable estimated and invalid terminals are excluded from confirmed totals"):
    val current = flow("current", start.plusSeconds(10), 17, 29)
    val excluded = List("partial", "unavailable", "estimated", "legacy_unknown").zipWithIndex
      .map((status, index) => flow(s"excluded-$index", start.plusSeconds(10), 9999, 9999, status))
    val invalid = flow("negative", start.plusSeconds(10), -1, 2)
    val unvalidated = flow("missing", start.plusSeconds(10), 9999, 9999, disposition = "missing_open")
    val result = ProxyBehaviorStatistics.transfer(current :: invalid :: unvalidated :: excluded, Nil, None)
    assertEquals(result.hcursor.get[BigInt]("bytes_up"), Right(BigInt(17)))
    assertEquals(result.hcursor.get[BigInt]("bytes_down"), Right(BigInt(29)))
    assertEquals(result.hcursor.get[Int]("excluded_flows"), Right(6))
    assertEquals(result.hcursor.get[Int]("measured_terminal_flows"), Right(1))
    assertEquals(result.hcursor.get[String]("baseline_disposition"), Right("insufficient_history"))
    assertEquals(result.hcursor.get[Boolean]("candidate"), Right(false))
    assert(result.hcursor.downField("new_to_retained_scope").focus.get.isNull)
    assert(ProxyBehaviorStatistics.transfer(excluded, history, None).hcursor.downField("bytes_up").focus.get.isNull)

  test("zero download and insufficient history produce explicit unavailable ratios and deviations"):
    val result = ProxyBehaviorStatistics.transfer(List(flow("zero-down", start, 500, 0)), history.take(5), Some(false))
    assertEquals(result.hcursor.get[String]("ratio_disposition"), Right("zero_download"))
    assert(result.hcursor.downField("upload_download_ratio").focus.get.isNull)
    assert(result.hcursor.downField("upload_profile").downField("maximum_upward_robust_deviation").focus.get.isNull)
    assertEquals(result.hcursor.get[Int]("zero_download_flows"), Right(1))
    assertEquals(result.hcursor.get[Boolean]("candidate"), Right(false))
    assertEquals(result.hcursor.downField("duration_profile").get[Int]("historical_sample_size"), Right(5))

  test("ratio and connection duration retain separate historical deviations and robust benign controls"):
    val ratioChange = ProxyBehaviorStatistics.transfer(List(flow("ratio-change", start, 100, 20)), history, Some(false))
    assertEquals(ratioChange.hcursor.downField("ratio_profile").get[Boolean]("candidate"), Right(true))
    assertEquals(ratioChange.hcursor.downField("upload_profile").get[Boolean]("candidate"), Right(false))
    val longer = flow("longer", start).copy(evidence = flow("longer", start).evidence
      .deepMerge(Json.obj("duration_ms" -> 10000.asJson)))
    val durationChange = ProxyBehaviorStatistics.transfer(List(longer), history, None)
    assertEquals(durationChange.hcursor.downField("duration_profile").get[Boolean]("candidate"), Right(true))
    val headerLatency = longer.copy(evidence = longer.evidence.deepMerge(Json.obj("duration_status" -> "header_latency".asJson)))
    assertEquals(ProxyBehaviorStatistics.transfer(List(headerLatency), history, None)
      .hcursor.downField("duration_profile").get[String]("disposition"), Right("unavailable"))
    val historicalOutlier = flow("old-outlier", start.minusSeconds(60), 1000000, 2000000)
    val benign = ProxyBehaviorStatistics.transfer(List(flow("ordinary", start)), history :+ historicalOutlier, Some(false))
    assertEquals(benign.hcursor.get[Boolean]("candidate"), Right(false))
    assertEquals(benign.hcursor.downField("upload_profile").get[BigDecimal]("historical_mad"), Right(BigDecimal(0)))

  test("confirmed aggregate bytes do not overflow and conflicting flow copies are excluded"):
    val a = flow("a", start, Long.MaxValue, Long.MaxValue)
    val b = flow("b", start, Long.MaxValue, Long.MaxValue)
    val result = ProxyBehaviorStatistics.transfer(List(a, b), history, None)
    assertEquals(result.hcursor.get[BigInt]("bytes_up"), Right(BigInt(Long.MaxValue) * 2))
    assertEquals(result.hcursor.get[BigDecimal]("upload_download_ratio"), Right(BigDecimal(1)))
    val conflict = a.copy(evidence = a.evidence.deepMerge(Json.obj("bytes_up" -> 1.asJson)))
    val conflicted = ProxyBehaviorStatistics.transfer(List(a, conflict, b), history, None)
    assertEquals(conflicted.hcursor.get[Int]("measured_terminal_flows"), Right(1))
    assertEquals(conflicted.hcursor.get[Int]("excluded_flows"), Right(1))
    assertEquals(conflicted.hcursor.get[BigInt]("bytes_up"), Right(BigInt(Long.MaxValue)))
