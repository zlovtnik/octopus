package com.sslproxy.coordinator.metrics

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.sslproxy.coordinator.observability.{CoordinatorMetrics, StructuredLogger}
import com.sslproxy.coordinator.util.ErrorSanitizer
import io.circe.Json

import java.time.{Instant, ZoneOffset}
import java.time.temporal.ChronoUnit

final case class MeasuredMetric[A](value: A, computedAt: Instant)

/** Compute-only metrics materializer. Jobs write last-good parts in memory and
  * publish a full snapshot to Redis/MinIO. Never participates in processor
  * readiness and never fails the process.
  */
final class StatsMaterializer(
  repo: MetricsRepository[IO],
  metrics: CoordinatorMetrics,
  redis: StatsSnapshotStore,
  minio: StatsSnapshotStore & StatsHistoryStore,
  dayState: Ref[IO, Option[MeasuredMetric[Option[DayPeak]]]],
  weekState: Ref[IO, Option[MeasuredMetric[Option[WeekPeak]]]],
  lifetimeState: Ref[IO, Option[LifetimeTotals]],
  dayHistory: Ref[IO, Option[MeasuredMetric[ThroughputSeries]]],
  weekHistory: Ref[IO, Option[MeasuredMetric[ThroughputSeries]]],
  liveState: Ref[IO, Option[LiveStripSnapshot]],
  now: IO[Instant]
):
  private val logger = StructuredLogger("metrics.materializer")

  def runJob(job: MetricJob): IO[Unit] =
    job match
      case MetricJob.Peaks          => refreshPeaks
      case MetricJob.LiveStrip      => refreshLiveStrip
      case MetricJob.HistoryBuckets => refreshHistory
      case MetricJob.Publish        => publish

  private def refreshPeaks: IO[Unit] =
    now.flatMap { at =>
      (
        recover("peakDay")(repo.peakDay.flatMap(v => update(dayState, MeasuredMetric(v, at)))),
        recover("peakWeek")(repo.peakWeek.flatMap(v => update(weekState, MeasuredMetric(v, at)))),
        recover("lifetimeTotals")(
          repo.lifetimeTotals(at).flatMap { totals =>
            lifetimeState.update {
              case Some(previous) if Instant.parse(previous.computedAt).isAfter(at) => Some(previous)
              case _ => Some(totals)
            }
          }
        )
      ).parTupled.void
    }

  private def recover(label: String)(run: IO[Unit]): IO[Unit] =
    run.handleErrorWith(error => IO(logger.warn(
      "metric_query_failed",
      "metric" -> label,
      "error" -> ErrorSanitizer.message(error)
    )))

  // Overlapping jobs may finish out of order; an older refresh cannot regress a cache.
  private def update[A](state: Ref[IO, Option[MeasuredMetric[A]]], value: MeasuredMetric[A]): IO[Unit] =
    state.update {
      case Some(previous) if previous.computedAt.isAfter(value.computedAt) => Some(previous)
      case _ => Some(value)
    }

  private def refreshLiveStrip: IO[Unit] =
    now.flatMap { now =>
      val rate = metrics.ingestProcessedRatePerSec(now.toEpochMilli)
      val pending = metrics.pendingLedgerCountValue
      val lastSuccess = metrics.ingestLastSuccessEpochSeconds.map(ts => StatsSnapshot.toIso(Instant.ofEpochSecond(ts)))
      val backpressure = metrics.backpressureActiveValue
      val strip =
        if metrics.publicReadingsFresh(now.toEpochMilli) then
          Some(
            LiveStripSnapshot(
              ingestProcessedRatePerSec = rate,
              pendingLedgerCount = pending,
              lastIngestSuccessAt = lastSuccess,
              backpressureActive = backpressure
            )
          )
        else None
      liveState.set(strip)
    }

  private def refreshHistory: IO[Unit] =
    now.flatMap { at =>
      val until = at.truncatedTo(ChronoUnit.HOURS)
      def refresh(hours: Int, state: Ref[IO, Option[MeasuredMetric[ThroughputSeries]]]): IO[Unit] =
        recover(s"throughput${hours}h") {
          repo.hourlyBuckets(until.minus(hours.toLong, ChronoUnit.HOURS), until).flatMap { rows =>
            val series = ThroughputSeries("hour", StatsMaterializer.fillHourly(rows, at, hours))
            update(state, MeasuredMetric(series, at))
          }
        }
      (refresh(24, dayHistory), refresh(168, weekHistory)).parTupled.void
    }

  private def currentHistory(value: Option[MeasuredMetric[ThroughputSeries]], at: Instant): Option[ThroughputSeries] =
    // The wire contract has no per-series freshness field. Never relabel an old
    // hourly window as current, or fill unknown hours after a failed query with zeros.
    value.filter(_.computedAt.truncatedTo(ChronoUnit.HOURS) == at.truncatedTo(ChronoUnit.HOURS)).map(_.value)

  private def publish: IO[Unit] =
    for
      day <- dayState.get
      week <- weekState.get
      lifetime <- lifetimeState.get
      history24h <- dayHistory.get
      history7d <- weekHistory.get
      live <- liveState.get
      now <- now
      d = day.flatMap(_.value)
      w = week.flatMap(_.value)
      // A shared timestamp must not claim the older surviving peak is newer.
      peaksAt = (day.map(_.computedAt).toList ++ week.map(_.computedAt).toList).minOption
      peaks = peaksAt.map(at => PeaksSnapshot(
        Some(StatsSnapshot.toIso(at)), d.map(_.records), d.map(_.date),
        w.map(_.records), w.map(_.start), w.map(_.end)
      ))
      snapshot = StatsSnapshot(
        asOf = StatsSnapshot.toIso(now),
        peaks = peaks,
        liveStrip = live,
        lifetimeTotals = lifetime,
        throughput24h = currentHistory(history24h, now),
        throughput7d = currentHistory(history7d, now)
      )
      json = StatsSnapshot.toJson(snapshot)
      _ <- redis.save(json).handleErrorWith { error =>
        IO(
          logger.warn(
            "redis_publish_failed",
            "error" -> ErrorSanitizer.message(error)
          )
        )
      }
      _ <- minio.save(json).handleErrorWith { error =>
        IO(
          logger.warn(
            "minio_publish_failed",
            "error" -> ErrorSanitizer.message(error)
          )
        )
      }
      _ <- writeHistoryObjects(json, now).handleErrorWith { error =>
        IO(
          logger.warn(
            "minio_history_failed",
            "error" -> ErrorSanitizer.message(error)
          )
        )
      }
    yield logger.info("stats_published", "asOf" -> snapshot.asOf)

  private def writeHistoryObjects(json: Json, now: Instant): IO[Unit] =
    val date = now.atZone(ZoneOffset.UTC).toLocalDate
    val hour = now.atZone(ZoneOffset.UTC).getHour
    val hourPath = f"${date.getYear}%04d/${date.getMonthValue}%02d/${date.getDayOfMonth}%02d/stats-${date.getYear}%04d${date.getMonthValue}%02d${date.getDayOfMonth}%02dT$hour%02d.json"
    val dailyPath = f"${date.getYear}%04d/${date.getMonthValue}%02d/stats-${date.getYear}%04d-${date.getMonthValue}%02d-${date.getDayOfMonth}%02d.json"
    minio.putHistory(hourPath, json) *> minio.putDaily(dailyPath, json)

object StatsMaterializer:
  /** Dense hourly series ending at the last complete hour (oldest first). */
  def fillHourly(
    points: List[ThroughputPoint],
    now: Instant,
    hours: Int
  ): List[ThroughputPoint] =
    val lastComplete = now.atZone(ZoneOffset.UTC).truncatedTo(ChronoUnit.HOURS).minusHours(1)
    val byStart = points.map(p => p.bucketStart -> p.records).toMap
    (0 until hours).toList.reverse.map { offset =>
      val start = lastComplete.minusHours(offset.toLong)
      val key = StatsSnapshot.toIso(start.toInstant)
      ThroughputPoint(key, byStart.getOrElse(key, 0L))
    }

  def create(
    repo: MetricsRepository[IO],
    metrics: CoordinatorMetrics,
    redis: StatsSnapshotStore,
    minio: StatsSnapshotStore & StatsHistoryStore,
    now: IO[Instant] = IO.realTimeInstant
  ): IO[StatsMaterializer] =
    for
      day <- Ref.of[IO, Option[MeasuredMetric[Option[DayPeak]]]](None)
      week <- Ref.of[IO, Option[MeasuredMetric[Option[WeekPeak]]]](None)
      lifetime <- Ref.of[IO, Option[LifetimeTotals]](None)
      history24h <- Ref.of[IO, Option[MeasuredMetric[ThroughputSeries]]](None)
      history7d <- Ref.of[IO, Option[MeasuredMetric[ThroughputSeries]]](None)
      live <- Ref.of[IO, Option[LiveStripSnapshot]](None)
    yield new StatsMaterializer(repo, metrics, redis, minio, day, week, lifetime, history24h, history7d, live, now)
