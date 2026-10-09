package com.sslproxy.coordinator.metrics

import io.circe.Json
import munit.CatsEffectSuite

import java.time.Instant

class StatsSnapshotSuite extends CatsEffectSuite:

  test("toJson emits null peaks and no invented zeros") {
    val snap = StatsSnapshot.empty(Instant.parse("2026-10-08T12:00:00Z"))
    val json = StatsSnapshot.toJson(snap)
    assertEquals(json.hcursor.get[String]("asOf").toOption, Some("2026-10-08T12:00:00Z"))
    assert(json.hcursor.get[Long]("peakRecordsDay").isLeft)
    assert(json.hcursor.downField("peaksComputedAt").focus.exists(_.isNull))
    assert(json.hcursor.downField("liveStrip").focus.exists(_.isNull))
    assert(json.hcursor.downField("lifetimeTotals").focus.exists(_.isNull))
    assert(json.hcursor.downField("throughput24h").focus.exists(_.isNull))
    assert(json.hcursor.downField("throughput7d").focus.exists(_.isNull))
  }

  test("round-trip keeps peaks and series") {
    val snap = StatsSnapshot(
      asOf = "2026-10-08T12:00:00Z",
      peaks = Some(
        PeaksSnapshot(
          peaksComputedAt = Some("2026-10-08T11:55:00Z"),
          peakRecordsDay = Some(6048436L),
          peakRecordsDayDate = Some("2026-10-07"),
          peakRecordsWeek = Some(14926709L),
          peakRecordsWeekStart = Some("2026-10-05"),
          peakRecordsWeekEnd = Some("2026-10-11")
        )
      ),
      liveStrip = Some(
        LiveStripSnapshot(1.25, 10L, Some("2026-10-08T11:59:00Z"), false)
      ),
      lifetimeTotals = Some(LifetimeTotals(20L, 3L, "2026-10-08T11:55:00Z")),
      throughput24h = Some(
        ThroughputSeries("hour", List(ThroughputPoint("2026-10-08T11:00:00Z", 5L)))
      ),
      throughput7d = None
    )
    val json = StatsSnapshot.toJson(snap)
    val parsed = StatsSnapshot.parse(json)
    assert(parsed.isRight)
    val back = parsed.toOption.get
    assertEquals(back.asOf, snap.asOf)
    assertEquals(back.peaks.flatMap(_.peakRecordsDay), Some(6048436L))
    assertEquals(back.liveStrip.map(_.pendingLedgerCount), Some(10L))
    assertEquals(back.lifetimeTotals.map(_.recordsTotal), Some(20L))
    assertEquals(back.throughput24h.map(_.series.size), Some(1))
    assertEquals(back.throughput7d, None)
  }

  test("parse rejects missing asOf") {
    val json = Json.obj("peakRecordsDay" -> Json.fromLong(1L))
    assert(StatsSnapshot.parse(json).isLeft)
  }

  test("fillHourly is dense and chronological") {
    val now = Instant.parse("2026-10-08T12:30:00Z")
    val points = List(ThroughputPoint("2026-10-08T11:00:00Z", 7L))
    val filled = StatsMaterializer.fillHourly(points, now, 3)
    assertEquals(
      filled.map(_.bucketStart),
      List(
        "2026-10-08T09:00:00Z",
        "2026-10-08T10:00:00Z",
        "2026-10-08T11:00:00Z"
      )
    )
    assertEquals(filled.map(_.records), List(0L, 0L, 7L))
  }
