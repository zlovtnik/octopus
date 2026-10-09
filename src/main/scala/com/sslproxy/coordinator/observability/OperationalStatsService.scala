package com.sslproxy.coordinator.observability

import cats.effect.{IO, Ref}
import cats.effect.std.Semaphore
import cats.syntax.all.*
import com.sslproxy.coordinator.postgres.PostgresRepository

import java.time.Instant
import java.time.format.DateTimeFormatter
import scala.concurrent.duration.*

trait PeaksSource:
  def peakRecordsDay: IO[Option[(Long, String)]]
  def peakRecordsWeek: IO[Option[(Long, String, String)]]

object PostgresPeaksSource:
  def apply(repo: PostgresRepository): PeaksSource = new PeaksSource:
    def peakRecordsDay: IO[Option[(Long, String)]] =
      repo.peakRecordsDay().flatMap {
        case Right(v) => IO.pure(v)
        case Left(e)  => IO.raiseError(RuntimeException(s"peak day query failed: $e"))
      }
    def peakRecordsWeek: IO[Option[(Long, String, String)]] =
      repo.peakRecordsWeek().flatMap {
        case Right(v) => IO.pure(v)
        case Left(e)  => IO.raiseError(RuntimeException(s"peak week query failed: $e"))
      }

final case class PeaksSnapshot(
  peakRecordsDay: Option[Long],
  peakRecordsDayDate: Option[String],
  peakRecordsWeek: Option[Long],
  peakRecordsWeekStart: Option[String],
  peakRecordsWeekEnd: Option[String],
  computedAt: Instant
)

final case class LiveStrip(
  ingestProcessedRatePerSec: Double,
  pendingLedgerCount: Long,
  lastIngestSuccessAt: Option[String],
  backpressureActive: Boolean
)

final case class PublicStats(
  asOf: String,
  peaksComputedAt: Option[String],
  peakRecordsDay: Option[Long],
  peakRecordsDayDate: Option[String],
  peakRecordsWeek: Option[Long],
  peakRecordsWeekStart: Option[String],
  peakRecordsWeekEnd: Option[String],
  liveStrip: Option[LiveStrip]
)

object OperationalStatsService:
  private val isoFormatter = DateTimeFormatter.ISO_INSTANT

  def toIso(instant: Instant): String = isoFormatter.format(instant)

class OperationalStatsService(
  peaks: PeaksSource,
  metrics: CoordinatorMetrics,
  refreshEvery: FiniteDuration
):
  import OperationalStatsService.*

  private val cache: Ref[IO, Option[PeaksSnapshot]] = Ref.unsafe(None)
  private val refreshLock: IO[Semaphore[IO]] = Semaphore[IO](1)

  def snapshot: IO[PublicStats] =
    for
      cached <- cache.get
      now <- IO(Instant.now())
      result <- cached match
        case Some(s) if java.time.Duration.between(s.computedAt, now).toSeconds < refreshEvery.toSeconds =>
          IO.pure(s)
        case _ =>
          refreshLock.flatMap(_.permit.use(_ => refreshIfNeeded(now)))
      live = liveStrip(now)
    yield buildStats(result, live, now)

  private def refreshIfNeeded(now: Instant): IO[PeaksSnapshot] =
    cache.get.flatMap {
      case Some(s) if java.time.Duration.between(s.computedAt, now).toSeconds < refreshEvery.toSeconds =>
        IO.pure(s)
      case _ => refresh(now)
    }

  private def refresh(now: Instant): IO[PeaksSnapshot] =
    (peaks.peakRecordsDay, peaks.peakRecordsWeek).parTupled.flatMap {
      case (day, week) =>
        val snap = PeaksSnapshot(
          peakRecordsDay = day.map(_._1),
          peakRecordsDayDate = day.map(_._2),
          peakRecordsWeek = week.map(_._1),
          peakRecordsWeekStart = week.map(_._2),
          peakRecordsWeekEnd = week.map(_._3),
          computedAt = now
        )
        cache.set(Some(snap)) *> IO.pure(snap)
    }.handleErrorWith { _ =>
      cache.get.flatMap {
        case Some(s) => IO.pure(s)
        case None    => IO.raiseError(RuntimeException("peak query failed and no cached value"))
      }
    }

  private def liveStrip(now: Instant): Option[LiveStrip] =
    val rate = metrics.ingestProcessedRatePerSec(now.toEpochMilli)
    val pending = metrics.pendingLedgerCountValue
    val lastSuccess = metrics.ingestLastSuccessEpochSeconds.map { ts =>
      Instant.ofEpochSecond(ts).toString
    }
    val backpressure = metrics.backpressureActiveValue
    Some(LiveStrip(rate, pending, lastSuccess, backpressure))

  private def buildStats(
    peaks: PeaksSnapshot,
    live: Option[LiveStrip],
    now: Instant
  ): PublicStats =
    PublicStats(
      asOf = toIso(now),
      peaksComputedAt = Some(toIso(peaks.computedAt)),
      peakRecordsDay = peaks.peakRecordsDay,
      peakRecordsDayDate = peaks.peakRecordsDayDate,
      peakRecordsWeek = peaks.peakRecordsWeek,
      peakRecordsWeekStart = peaks.peakRecordsWeekStart,
      peakRecordsWeekEnd = peaks.peakRecordsWeekEnd,
      liveStrip = live
    )
