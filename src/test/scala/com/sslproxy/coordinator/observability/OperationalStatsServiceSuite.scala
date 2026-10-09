package com.sslproxy.coordinator.observability

import cats.effect.{IO, Deferred}
import cats.syntax.all.*
import munit.CatsEffectSuite
import scala.concurrent.duration.*

class OperationalStatsServiceSuite extends CatsEffectSuite:
  private def stubPeaks: PeaksSource = new PeaksSource:
    def peakRecordsDay = IO.pure(Some((100L, "2026-10-07")))
    def peakRecordsWeek = IO.pure(Some((200L, "2026-10-05", "2026-10-11")))

  test("snapshot returns measured peaks and no unobserved process defaults"):
    OperationalStatsService.create(stubPeaks, CoordinatorMetrics(), 60.seconds).flatMap(_.snapshot).map { s =>
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
      _ <- service.snapshot.background.use { first =>
        entered.get *> List.fill(8)(service.snapshot).parSequence.background.use { others =>
          release.complete(()) *> first.flatMap(_.embedNever) *> others.flatMap(_.embedNever).void
        }
      }.guarantee(release.complete(()).void)
    yield assertEquals(calls.get(), 1)

  test("expired peaks are never republished after a database failure"):
    val failNow = new java.util.concurrent.atomic.AtomicBoolean(false)
    val source = new PeaksSource:
      def peakRecordsDay = IO.defer(if failNow.get() then IO.raiseError(RuntimeException("db down")) else stubPeaks.peakRecordsDay)
      def peakRecordsWeek = stubPeaks.peakRecordsWeek
    for
      service <- OperationalStatsService.create(source, CoordinatorMetrics(), 0.seconds)
      first <- service.snapshot
      _ = failNow.set(true)
      second <- service.snapshot.attempt
    yield
      assertEquals(first.peakRecordsDay, Some(100L))
      assert(second.isLeft)

  test("live strip uses fresh observations after the five-minute window warms up"):
    val now = new java.util.concurrent.atomic.AtomicLong(System.currentTimeMillis() - 301_000L)
    val metrics = new CoordinatorMetrics(new io.micrometer.core.instrument.simple.SimpleMeterRegistry(), () => now.get())
    now.set(System.currentTimeMillis())
    metrics.recordPendingLedgerCount(42)
    metrics.recordBackpressureActive(true)
    metrics.recordIngestInvocation(success = true)
    metrics.recordIngestProcessed(300)
    OperationalStatsService.create(stubPeaks, metrics, 60.seconds).flatMap(_.snapshot).map { s =>
      assertEquals(s.liveStrip.map(_.pendingLedgerCount), Some(42L))
      assertEquals(s.liveStrip.map(_.ingestProcessedRatePerSec), Some(1.0))
      assert(s.liveStrip.exists(_.backpressureActive))
    }
