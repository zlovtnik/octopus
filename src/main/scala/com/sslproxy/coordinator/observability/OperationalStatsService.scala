package com.sslproxy.coordinator.observability

import cats.effect.{IO, Ref}
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

/** Read side used by `PublicStatsRoutes`; kept as a trait so the HTTP ceiling
  * can be tested without a live database or a real refresh path.
  */
trait PublicStatsSource:
  def snapshot: IO[PublicStats]

object OperationalStatsService:
  private val isoFormatter = DateTimeFormatter.ISO_INSTANT

  def toIso(instant: Instant): String = isoFormatter.format(instant)

  def create(peaks: PeaksSource, metrics: CoordinatorMetrics, refreshEvery: FiniteDuration): IO[OperationalStatsService] =
    for
      cache <- Ref.of[IO, Option[PeaksSnapshot]](None)
      refreshInFlight <- Ref.of[IO, Boolean](false)
    yield new OperationalStatsService(peaks, metrics, refreshEvery, cache, refreshInFlight)

class OperationalStatsService private (
  peaks: PeaksSource,
  metrics: CoordinatorMetrics,
  refreshEvery: FiniteDuration,
  cache: Ref[IO, Option[PeaksSnapshot]],
  refreshInFlight: Ref[IO, Boolean]
) extends PublicStatsSource:
  import OperationalStatsService.*

  /** Never fails on peaks lookup. Returns the last good snapshot when a refresh
    * is pending or has just failed; peak fields are null only before the first
    * successful refresh.
    */
  def snapshot: IO[PublicStats] =
    for
      cached <- cache.get
      now <- IO(Instant.now())
      _ <- startRefreshIfStale(cached, now)
      sampledAt <- IO(Instant.now())
      live = liveStrip(sampledAt)
    yield buildStats(cached, live, sampledAt)

  private def isStale(snap: Option[PeaksSnapshot], now: Instant): Boolean =
    snap.forall(s => java.time.Duration.between(s.computedAt, now).toSeconds >= refreshEvery.toSeconds)

  private def startRefreshIfStale(cached: Option[PeaksSnapshot], now: Instant): IO[Unit] =
    IO.whenA(isStale(cached, now)) {
      refreshInFlight
        .modify {
          case true  => (true, false)
          case false => (true, true)
        }
        .flatMap { shouldStart =>
          IO.whenA(shouldStart) {
            refresh(now).guarantee(refreshInFlight.set(false)).start.void
          }
        }
    }

  private def refresh(now: Instant): IO[Unit] =
    (peaks.peakRecordsDay, peaks.peakRecordsWeek).parTupled
      .map { case (day, week) =>
        PeaksSnapshot(
          peakRecordsDay = day.map(_._1),
          peakRecordsDayDate = day.map(_._2),
          peakRecordsWeek = week.map(_._1),
          peakRecordsWeekStart = week.map(_._2),
          peakRecordsWeekEnd = week.map(_._3),
          computedAt = now
        )
      }
      .flatMap(snap => cache.set(Some(snap)))
      // Keep the previous snapshot (if any) when the database is slow or down.
      .handleErrorWith(_ => IO.unit)

  private def liveStrip(now: Instant): Option[LiveStrip] =
    val rate = metrics.ingestProcessedRatePerSec(now.toEpochMilli)
    val pending = metrics.pendingLedgerCountValue
    val lastSuccess = metrics.ingestLastSuccessEpochSeconds.map { ts =>
      Instant.ofEpochSecond(ts).toString
    }
    val backpressure = metrics.backpressureActiveValue
    Option.when(metrics.publicReadingsFresh(now.toEpochMilli))(LiveStrip(rate, pending, lastSuccess, backpressure))

  private def buildStats(
    peaks: Option[PeaksSnapshot],
    live: Option[LiveStrip],
    now: Instant
  ): PublicStats =
    PublicStats(
      asOf = toIso(now),
      peaksComputedAt = peaks.map(s => toIso(s.computedAt)),
      peakRecordsDay = peaks.flatMap(_.peakRecordsDay),
      peakRecordsDayDate = peaks.flatMap(_.peakRecordsDayDate),
      peakRecordsWeek = peaks.flatMap(_.peakRecordsWeek),
      peakRecordsWeekStart = peaks.flatMap(_.peakRecordsWeekStart),
      peakRecordsWeekEnd = peaks.flatMap(_.peakRecordsWeekEnd),
      liveStrip = live
    )
