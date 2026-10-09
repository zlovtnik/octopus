package com.sslproxy.coordinator.metrics

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.sslproxy.coordinator.observability.{CoordinatorMetrics, StructuredLogger}
import com.sslproxy.coordinator.postgres.PostgresRepository
import io.circe.Json

import java.time.{Instant, ZoneOffset}
import java.time.temporal.ChronoUnit

final case class MaterializedPeaks(
  peaks: PeaksSnapshot,
  lifetime: Option[LifetimeTotals],
  computedAt: Instant
)

final case class MaterializedHistory(
  throughput24h: Option[ThroughputSeries],
  throughput7d: Option[ThroughputSeries],
  computedAt: Instant
)

/** Compute-only metrics materializer. Jobs write last-good parts in memory and
  * publish a full snapshot to Redis/MinIO. Never participates in processor
  * readiness and never fails the process.
  */
final class StatsMaterializer(
  repo: PostgresRepository,
  metrics: CoordinatorMetrics,
  redis: StatsSnapshotStore,
  minio: StatsSnapshotStore & StatsHistoryStore,
  peaksState: Ref[IO, Option[MaterializedPeaks]],
  historyState: Ref[IO, Option[MaterializedHistory]],
  liveState: Ref[IO, Option[LiveStripSnapshot]]
):
  private val logger = StructuredLogger("metrics.materializer")

  def runJob(job: MetricJob): IO[Unit] =
    job match
      case MetricJob.Peaks          => refreshPeaks
      case MetricJob.LiveStrip      => refreshLiveStrip
      case MetricJob.HistoryBuckets => refreshHistory
      case MetricJob.Publish        => publish

  private def refreshPeaks: IO[Unit] =
    val day = repo.peakRecordsDay().flatMap {
      case Right(v) => IO.pure(v)
      case Left(e)  => IO.raiseError(RuntimeException(s"peak day failed: $e"))
    }
    val week = repo.peakRecordsWeek().flatMap {
      case Right(v) => IO.pure(v)
      case Left(e)  => IO.raiseError(RuntimeException(s"peak week failed: $e"))
    }
    val lifetime = repo.ingestionLifetimeTotals().flatMap {
      case Right(v) => IO.pure(v)
      case Left(e)  => IO.raiseError(RuntimeException(s"lifetime totals failed: $e"))
    }
    (day, week, lifetime).parTupled.flatMap { case (d, w, l) =>
      val now = Instant.now()
      val peaks = PeaksSnapshot(
        peaksComputedAt = Some(StatsSnapshot.toIso(now)),
        peakRecordsDay = d.map(_._1),
        peakRecordsDayDate = d.map(_._2),
        peakRecordsWeek = w.map(_._1),
        peakRecordsWeekStart = w.map(_._2),
        peakRecordsWeekEnd = w.map(_._3)
      )
      val totals = l.map { case (records, days) =>
        LifetimeTotals(recordsTotal = records, daysCounted = days, computedAt = StatsSnapshot.toIso(now))
      }
      peaksState.set(Some(MaterializedPeaks(peaks, totals, now)))
    }

  private def refreshLiveStrip: IO[Unit] =
    IO(Instant.now()).flatMap { now =>
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
    val since = Instant.now().minus(8, ChronoUnit.DAYS)
    repo.ingestionHourlyBuckets(since).flatMap {
      case Left(e)  => IO.raiseError(RuntimeException(s"history buckets failed: $e"))
      case Right(rows) =>
        val now = Instant.now()
        val points = rows.map { case (bucketStart, records) =>
          ThroughputPoint(bucketStart, records)
        }
        val full = StatsMaterializer.fillHourly(points, now, 168)
        historyState.set(
          Some(
            MaterializedHistory(
              throughput24h = Some(ThroughputSeries("hour", full.takeRight(24))),
              throughput7d = Some(ThroughputSeries("hour", full)),
              computedAt = now
            )
          )
        )
    }

  private def publish: IO[Unit] =
    for
      peaks <- peaksState.get
      history <- historyState.get
      live <- liveState.get
      now <- IO(Instant.now())
      snapshot = StatsSnapshot(
        asOf = StatsSnapshot.toIso(now),
        peaks = peaks.map(_.peaks),
        liveStrip = live,
        lifetimeTotals = peaks.flatMap(_.lifetime),
        throughput24h = history.flatMap(_.throughput24h),
        throughput7d = history.flatMap(_.throughput7d)
      )
      json = StatsSnapshot.toJson(snapshot)
      _ <- redis.save(json).handleErrorWith { error =>
        IO(
          logger.warn(
            "redis_publish_failed",
            "error" -> Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
          )
        )
      }
      _ <- minio.save(json).handleErrorWith { error =>
        IO(
          logger.warn(
            "minio_publish_failed",
            "error" -> Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
          )
        )
      }
      _ <- writeHistoryObjects(json, now).handleErrorWith { error =>
        IO(
          logger.warn(
            "minio_history_failed",
            "error" -> Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
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
    repo: PostgresRepository,
    metrics: CoordinatorMetrics,
    redis: StatsSnapshotStore,
    minio: StatsSnapshotStore & StatsHistoryStore
  ): IO[StatsMaterializer] =
    for
      peaks <- Ref.of[IO, Option[MaterializedPeaks]](None)
      history <- Ref.of[IO, Option[MaterializedHistory]](None)
      live <- Ref.of[IO, Option[LiveStripSnapshot]](None)
    yield new StatsMaterializer(repo, metrics, redis, minio, peaks, history, live)
