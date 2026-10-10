package com.sslproxy.coordinator.observability

import cats.effect.{IO, Deferred}
import cats.syntax.all.*
import munit.CatsEffectSuite
import scala.concurrent.duration.*

class OperationalStatsServiceSuite extends CatsEffectSuite:
  private def stubPeaks: PeaksSource = new PeaksSource:
    def peakRecordsDay = IO.pure(Some((100L, "2026-10-07")))
    def peakRecordsWeek = IO.pure(Some((200L, "2026-10-05", "2026-10-11")))

  private def awaitPeaks(service: OperationalStatsService): IO[PublicStats] =
    service.snapshot.flatMap { s =>
      if s.peaksComputedAt.isDefined then IO.pure(s)
      else IO.sleep(20.millis) *> awaitPeaks(service)
    }

  test("snapshot returns measured peaks and no unobserved process defaults"):
    OperationalStatsService.create(stubPeaks, CoordinatorMetrics(), 60.seconds).flatMap(awaitPeaks).map { s =>
      assertEquals(s.peakRecordsDay, Some(100L))
      assertEquals(s.peakRecordsWeek, Some(200L))
      assertEquals(s.liveStrip, None)
      assert(s.peaksComputedAt.nonEmpty)
    }

  test("concurrent requests share one peak refresh"):
    val calls = new java.util.concurrent.atomic.AtomicInteger(0)
    for
      entered <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      source = new PeaksSource:
        def peakRecordsDay = IO(calls.incrementAndGet()) *> entered.complete(()).void *> release.get.as(Some((100L, "2026-10-07")))
        def peakRecordsWeek = IO.pure(Some((200L, "2026-10-05", "2026-10-11")))
      service <- OperationalStatsService.create(source, CoordinatorMetrics(), 60.seconds)
      first <- service.snapshot
      _ <- entered.get
      others <- List.fill(8)(service.snapshot).parSequence
      _ <- release.complete(())
      filled <- awaitPeaks(service)
    yield
      assertEquals(first.peakRecordsDay, None)
      assert(others.forall(_.peakRecordsDay.isEmpty))
      assertEquals(filled.peakRecordsDay, Some(100L))
      assertEquals(calls.get(), 1)

  test("refresh failure serves the last good snapshot instead of raising"):
    val failNow = new java.util.concurrent.atomic.AtomicBoolean(false)
    val source = new PeaksSource:
      def peakRecordsDay = IO.defer(if failNow.get() then IO.raiseError(RuntimeException("db down")) else stubPeaks.peakRecordsDay)
      def peakRecordsWeek = IO.defer(if failNow.get() then IO.raiseError(RuntimeException("db down")) else stubPeaks.peakRecordsWeek)
    for
      service <- OperationalStatsService.create(source, CoordinatorMetrics(), 0.seconds)
      first <- awaitPeaks(service)
      _ = failNow.set(true)
      _ <- IO.sleep(50.millis)
      second <- service.snapshot
    yield
      assertEquals(first.peakRecordsDay, Some(100L))
      assertEquals(second.peakRecordsDay, Some(100L))
      assertEquals(second.peakRecordsWeek, Some(200L))
      assert(second.peaksComputedAt.nonEmpty)

  test("first snapshot with a database failure returns null peaks without raising"):
    val source = new PeaksSource:
      def peakRecordsDay = IO.raiseError(RuntimeException("db down"))
      def peakRecordsWeek = IO.raiseError(RuntimeException("db down"))
    for
      service <- OperationalStatsService.create(source, CoordinatorMetrics(), 60.seconds)
      s <- service.snapshot
    yield
      assertEquals(s.peakRecordsDay, None)
      assertEquals(s.peakRecordsDayDate, None)
      assertEquals(s.peakRecordsWeek, None)
      assertEquals(s.peaksComputedAt, None)

  test("live strip uses fresh observations after the five-minute window warms up"):
    val now = new java.util.concurrent.atomic.AtomicLong(System.currentTimeMillis() - 301_000L)
    val metrics = CoordinatorMetrics.withClock(() => now.get())
    now.set(System.currentTimeMillis())
    metrics.recordPendingLedgerCount(42)
    metrics.recordBackpressureActive(true)
    metrics.recordIngestInvocation(success = true)
    metrics.recordIngestProcessed(0)
    metrics.recordBrokerRecordsCommitted(300)
    OperationalStatsService.create(stubPeaks, metrics, 60.seconds).flatMap(awaitPeaks).map { s =>
      assertEquals(s.liveStrip.map(_.pendingLedgerCount), Some(42L))
      assertEquals(s.liveStrip.map(_.ingestProcessedRatePerSec), Some(1.0))
      assert(s.liveStrip.exists(_.backpressureActive))
    }
