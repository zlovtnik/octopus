package com.sslproxy.coordinator.metrics

import cats.effect.{Deferred, IO, Ref, Resource}
import cats.syntax.all.*
import com.sslproxy.coordinator.observability.CoordinatorMetrics
import io.circe.Json
import munit.CatsEffectSuite

import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.concurrent.duration.*

class StatsMaterializerSuite extends CatsEffectSuite:
  private val at = Instant.parse("2026-10-08T12:30:00Z")
  private val broken = new java.sql.SQLException("synthetic failure", "57014")

  private class Repository extends MetricsRepository[IO]:
    def peakDay: IO[Option[DayPeak]] = IO.pure(Some(DayPeak(10L, "2026-10-08")))
    def peakWeek: IO[Option[WeekPeak]] = IO.pure(Some(WeekPeak(20L, "2026-10-05", "2026-10-11")))
    def lifetimeTotals(computedAt: Instant): IO[LifetimeTotals] =
      IO.pure(LifetimeTotals(30L, 3L, StatsSnapshot.toIso(computedAt)))
    def hourlyBuckets(from: Instant, until: Instant): IO[List[ThroughputPoint]] =
      IO.pure(List(ThroughputPoint(StatsSnapshot.toIso(until.minus(1, ChronoUnit.HOURS)), 5L)))

  private class Store(state: Ref[IO, Option[Json]]) extends StatsSnapshotStore with StatsHistoryStore:
    def load: IO[Option[Json]] = state.get
    def save(json: Json): IO[Unit] = state.set(Some(json))
    def putHistory(path: String, json: Json): IO[Unit] = IO.unit
    def putDaily(path: String, json: Json): IO[Unit] = IO.unit

  private def setup(repo: MetricsRepository[IO], clock: IO[Instant] = IO.pure(at)) =
    for
      state <- Ref.of[IO, Option[Json]](None)
      store = new Store(state)
      service <- StatsMaterializer.create(repo, CoordinatorMetrics(), store, store, clock)
    yield (service, store)

  private def snapshot(service: StatsMaterializer, store: Store): IO[StatsSnapshot] =
    service.runJob(MetricJob.Publish) *> store.load.map { value =>
      StatsSnapshot.parse(value.getOrElse(fail("snapshot was not published")))
        .fold(error => fail(error), identity)
    }

  test("a failed peak query preserves successful week and lifetime measurements"):
    val repo = new Repository:
      override def peakDay = IO.raiseError(broken)
    setup(repo).flatMap { (service, store) =>
      service.runJob(MetricJob.Peaks) *> snapshot(service, store).map { s =>
        assertEquals(s.peaks.flatMap(_.peakRecordsDay), None)
        assertEquals(s.peaks.flatMap(_.peakRecordsWeek), Some(20L))
        assertEquals(s.lifetimeTotals.map(_.recordsTotal), Some(30L))
      }
    }

  test("a failed seven-day query does not discard 24-hour throughput"):
    val repo = new Repository:
      override def hourlyBuckets(from: Instant, until: Instant) =
        if ChronoUnit.HOURS.between(from, until) == 168 then IO.raiseError(broken)
        else super.hourlyBuckets(from, until)
    setup(repo).flatMap { (service, store) =>
      service.runJob(MetricJob.HistoryBuckets) *> snapshot(service, store).map { s =>
        assertEquals(s.throughput7d, None)
        assertEquals(s.throughput24h.map(_.series.size), Some(24))
        assertEquals(s.throughput24h.map(_.series.last.records), Some(5L))
      }
    }

  test("cold-start failures stay null; successful empty history yields dense measured zeros"):
    for
      failed <- Ref.of[IO, Boolean](true)
      repo = new Repository:
        override def peakDay = IO.pure(None)
        override def peakWeek = IO.pure(None)
        override def lifetimeTotals(computedAt: Instant) =
          failed.get.flatMap {
            case true => IO.raiseError(broken)
            case false => IO.pure(LifetimeTotals(0, 0, StatsSnapshot.toIso(computedAt)))
          }
        override def hourlyBuckets(from: Instant, until: Instant) =
          failed.get.flatMap {
            case true => IO.raiseError(broken)
            case false => IO.pure(Nil)
          }
      pair <- setup(repo)
      (service, store) = pair
      _ <- (service.runJob(MetricJob.Peaks), service.runJob(MetricJob.HistoryBuckets)).parTupled
      missing <- snapshot(service, store)
      _ <- failed.set(false)
      _ <- (service.runJob(MetricJob.Peaks), service.runJob(MetricJob.HistoryBuckets)).parTupled
      empty <- snapshot(service, store)
    yield
      assertEquals(missing.lifetimeTotals, None)
      assertEquals(missing.throughput24h, None)
      assertEquals(missing.throughput7d, None)
      assertEquals(empty.lifetimeTotals.map(_.recordsTotal), Some(0L))
      assertEquals(empty.throughput24h.map(_.series.size), Some(24))
      assertEquals(empty.throughput7d.map(_.series.size), Some(168))
      assert(empty.throughput7d.exists(_.series.forall(_.records == 0L)))

  test("failed refresh retains last-good values and timestamps without fabricating the next hour"):
    for
      failed <- Ref.of[IO, Boolean](false)
      clock <- Ref.of[IO, Instant](at)
      repo = new Repository:
        override def lifetimeTotals(computedAt: Instant) =
          failed.get.flatMap(f => if f then IO.raiseError(broken) else super.lifetimeTotals(computedAt))
        override def hourlyBuckets(from: Instant, until: Instant) =
          failed.get.flatMap(f => if f then IO.raiseError(broken) else super.hourlyBuckets(from, until))
      pair <- setup(repo, clock.get)
      (service, store) = pair
      _ <- (service.runJob(MetricJob.Peaks), service.runJob(MetricJob.HistoryBuckets)).parTupled
      before <- snapshot(service, store)
      _ <- failed.set(true) *> clock.set(at.plusSeconds(60))
      _ <- (service.runJob(MetricJob.Peaks), service.runJob(MetricJob.HistoryBuckets)).parTupled
      fallback <- snapshot(service, store)
      _ <- clock.set(at.plus(1, ChronoUnit.HOURS))
      nextHour <- snapshot(service, store)
    yield
      assertEquals(fallback.lifetimeTotals, before.lifetimeTotals)
      assertEquals(fallback.throughput24h, before.throughput24h)
      assertEquals(fallback.throughput7d, before.throughput7d)
      assertEquals(nextHour.throughput24h, None)
      assertEquals(nextHour.throughput7d, None)

  test("history queries start concurrently with identical UTC cutoffs"):
    for
      bothStarted <- Deferred[IO, Unit]
      windows <- Ref.of[IO, List[(Instant, Instant)]](Nil)
      repo = new Repository:
        override def hourlyBuckets(from: Instant, until: Instant) =
          windows.updateAndGet((from, until) :: _).flatMap { seen =>
            IO.whenA(seen.size == 2)(bothStarted.complete(()).void) *> bothStarted.get.as(Nil)
          }
      pair <- setup(repo)
      _ <- pair._1.runJob(MetricJob.HistoryBuckets).timeout(2.seconds)
      seen <- windows.get
    yield
      assertEquals(seen.map(_._2).distinct, List(at.truncatedTo(ChronoUnit.HOURS)))
      assertEquals(seen.map((from, until) => ChronoUnit.HOURS.between(from, until)).sorted, List(24L, 168L))

  test("successful lifetime is published while sibling peaks remain in flight"):
    for
      started <- Deferred[IO, Unit]
      repo = new Repository:
        override def peakDay = started.complete(()).void *> IO.never
        override def peakWeek = IO.never
      pair <- setup(repo)
      (service, store) = pair
      result <- service.runJob(MetricJob.Peaks).background.use { _ =>
        def awaitLifetime: IO[StatsSnapshot] =
          snapshot(service, store).flatMap { s =>
            if s.lifetimeTotals.nonEmpty then IO.pure(s)
            else IO.cede *> awaitLifetime
          }
        started.get *> awaitLifetime.timeout(2.seconds)
      }
    yield assertEquals(result.lifetimeTotals.map(_.recordsTotal), Some(30L))

  test("an older refresh finishing late cannot overwrite newer lifetime totals"):
    for
      started <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      clock <- Ref.of[IO, Instant](at)
      repo = new Repository:
        override def lifetimeTotals(computedAt: Instant) =
          if computedAt == at then started.complete(()).void *> release.get *> super.lifetimeTotals(computedAt)
          else IO.pure(LifetimeTotals(40L, 3L, StatsSnapshot.toIso(computedAt)))
      pair <- setup(repo, clock.get)
      (service, store) = pair
      result <- Resource.make(service.runJob(MetricJob.Peaks).start)(_.cancel).use { fiber =>
        started.get *> clock.set(at.plusSeconds(60)) *>
          service.runJob(MetricJob.Peaks) *> release.complete(()) *>
          fiber.joinWithNever *> snapshot(service, store)
      }
    yield
      assertEquals(result.lifetimeTotals.map(_.recordsTotal), Some(40L))
      assertEquals(result.lifetimeTotals.map(_.computedAt), Some(StatsSnapshot.toIso(at.plusSeconds(60))))
