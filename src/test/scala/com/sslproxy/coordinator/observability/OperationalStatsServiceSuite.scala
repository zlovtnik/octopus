package com.sslproxy.coordinator.observability

import cats.effect.IO
import munit.CatsEffectSuite

import scala.concurrent.duration.*

class OperationalStatsServiceSuite extends CatsEffectSuite:

  private def stubPeaks: PeaksSource = new PeaksSource:
    def peakRecordsDay = IO.pure(Some((100L, "2026-10-07")))
    def peakRecordsWeek = IO.pure(Some((200L, "2026-10-05", "2026-10-11")))

  private def failingPeaks: PeaksSource = new PeaksSource:
    def peakRecordsDay = IO.raiseError(RuntimeException("db down"))
    def peakRecordsWeek = IO.raiseError(RuntimeException("db down"))

  test("snapshot returns peaks and live strip"):
    val service = new OperationalStatsService(stubPeaks, CoordinatorMetrics(), 60.seconds)
    service.snapshot.map { s =>
      assertEquals(s.peakRecordsDay, Some(100L))
      assertEquals(s.peakRecordsDayDate, Some("2026-10-07"))
      assertEquals(s.peakRecordsWeek, Some(200L))
      assertEquals(s.peakRecordsWeekStart, Some("2026-10-05"))
      assertEquals(s.peakRecordsWeekEnd, Some("2026-10-11"))
      assert(s.liveStrip.isDefined)
      assertEquals(s.liveStrip.get.pendingLedgerCount, 0L)
      assert(s.peaksComputedAt.isDefined)
    }

  test("snapshot caches peaks within TTL"):
    val counter = new java.util.concurrent.atomic.AtomicInteger(0)
    val source = new PeaksSource:
      def peakRecordsDay = IO(counter.incrementAndGet()) *> IO.pure(Some((100L, "2026-10-07")))
      def peakRecordsWeek = IO(counter.incrementAndGet()) *> IO.pure(Some((200L, "2026-10-05", "2026-10-11")))
    val service = new OperationalStatsService(source, CoordinatorMetrics(), 60.seconds)
    for
      _ <- service.snapshot
      _ <- service.snapshot
      _ <- service.snapshot
    yield assertEquals(counter.get(), 2, "peak queries should run only once within TTL")

  test("snapshot falls back to cached peaks on DB error after first success"):
    val failNow = new java.util.concurrent.atomic.AtomicBoolean(false)
    val source = new PeaksSource:
      def peakRecordsDay =
        if failNow.get() then IO.raiseError(RuntimeException("db down"))
        else IO.pure(Some((100L, "2026-10-07")))
      def peakRecordsWeek =
        if failNow.get() then IO.raiseError(RuntimeException("db down"))
        else IO.pure(Some((200L, "2026-10-05", "2026-10-11")))
    val service = new OperationalStatsService(source, CoordinatorMetrics(), 0.seconds)
    for
      first <- service.snapshot
      _ = failNow.set(true)
      second <- service.snapshot
    yield
      assertEquals(first.peakRecordsDay, Some(100L))
      assertEquals(second.peakRecordsDay, Some(100L), "should serve last-good on DB error")

  test("snapshot raises error when DB fails and no cached value exists"):
    val service = new OperationalStatsService(failingPeaks, CoordinatorMetrics(), 60.seconds)
    service.snapshot.attempt.map { result =>
      assert(result.isLeft, "should raise when no cached peaks and DB fails")
    }

  test("snapshot includes live strip from metrics"):
    val metrics = CoordinatorMetrics()
    metrics.recordPendingLedgerCount(42)
    metrics.recordBackpressureActive(true)
    metrics.recordIngestInvocation(success = true)
    val service = new OperationalStatsService(stubPeaks, metrics, 60.seconds)
    service.snapshot.map { s =>
      assert(s.liveStrip.isDefined)
      assertEquals(s.liveStrip.get.pendingLedgerCount, 42L)
      assert(s.liveStrip.get.backpressureActive)
      assert(s.liveStrip.get.lastIngestSuccessAt.nonEmpty)
    }
