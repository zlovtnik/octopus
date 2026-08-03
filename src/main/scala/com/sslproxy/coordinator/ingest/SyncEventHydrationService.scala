package com.sslproxy.coordinator.ingest

import cats.effect.IO
import cats.effect.std.Semaphore
import cats.syntax.all.*
import com.sslproxy.coordinator.observability.{CoordinatorMetrics, StructuredLogger}
import com.sslproxy.coordinator.tidb.{
  SyncEventHydrationCandidate,
  TidbPayloadReadException,
  TidbPayloadResolver,
  TidbRepository
}
import fs2.Stream

import scala.concurrent.duration.*

final class SyncEventHydrationService(
    repository: TidbRepository,
    payloadResolver: TidbPayloadResolver,
    metrics: CoordinatorMetrics,
    pageSize: Int,
    dbSemaphore: Semaphore[IO]
):
  import SyncEventHydrationService.log

  private final case class Stats(
      scanned: Long,
      hydrated: Long,
      skipped: Long,
      failed: Long
  ):
    def add(result: Either[Throwable, Boolean]): Stats =
      result match
        case Right(true)  => copy(scanned = scanned + 1, hydrated = hydrated + 1)
        case Right(false) => copy(scanned = scanned + 1, skipped = skipped + 1)
        case Left(_)      => copy(scanned = scanned + 1, failed = failed + 1)

  val runOnce: Stream[IO, Unit] =
    Stream.eval(loop(None, Stats(0, 0, 0, 0)))

  private def loop(
      after: Option[SyncEventHydrationCandidate],
      stats: Stats
  ): IO[Unit] =
    dbSemaphore.permit.use { _ =>
      repository.findSyncEventsNeedingHydration(after, pageSize)
    }.flatMap {
      case Left(error) =>
        IO.raiseError(new RuntimeException(
          s"${error.operation}: ${error.message}",
          error.cause
        ))
      case Right(Nil) =>
        IO(metrics.recordSyncEventHydrationBackfill(stats.hydrated, stats.failed)) *>
          IO(log.info(
            "sync_event_hydration_backfill",
            "status" -> "completed",
            "scanned" -> stats.scanned.toString,
            "hydrated" -> stats.hydrated.toString,
            "skipped" -> stats.skipped.toString,
            "failed" -> stats.failed.toString
          ))
      case Right(candidates) =>
        candidates.traverse(hydrateOne).flatMap { results =>
          val nextStats = results.foldLeft(stats)((current, result) => current.add(result))
          candidates.lastOption match
            case Some(next) if after.forall(cursorAdvances(_, next)) =>
              loop(Some(next), nextStats)
            case _ =>
              complete(nextStats, "non_advancing_cursor")
        }
    }

  private def complete(stats: Stats, status: String): IO[Unit] =
    IO(metrics.recordSyncEventHydrationBackfill(stats.hydrated, stats.failed)) *>
      IO(log.info(
        "sync_event_hydration_backfill",
        "status" -> status,
        "scanned" -> stats.scanned.toString,
        "hydrated" -> stats.hydrated.toString,
        "skipped" -> stats.skipped.toString,
        "failed" -> stats.failed.toString
      ))

  private def cursorAdvances(
      previous: SyncEventHydrationCandidate,
      next: SyncEventHydrationCandidate
  ): Boolean =
    val observed = next.observedAt.compareTo(previous.observedAt)
    observed > 0 ||
      (observed == 0 && next.streamName.compareTo(previous.streamName) > 0) ||
      (observed == 0 && next.streamName == previous.streamName &&
        next.dedupeKey.compareTo(previous.dedupeKey) > 0)

  private def hydrateOne(
      candidate: SyncEventHydrationCandidate
  ): IO[Either[Throwable, Boolean]] =
    val payloadIO = candidate.payloadJson match
      case Some(payload) => IO.pure(payload)
      case None          => resolveWithRetry(candidate.payloadRef, attempt = 1)

    (for
      payload <- payloadIO
      result <- dbSemaphore.permit.use { _ =>
        repository.hydrateExistingSyncEvent(candidate, payload)
      }
      hydrated <- result.fold(
        error => IO.raiseError[Boolean](new RuntimeException(
          s"${error.operation}: ${error.message}",
          error.cause
        )),
        IO.pure
      )
    yield hydrated).attempt.flatTap {
      case Right(_) => IO.unit
      case Left(error) =>
        IO(log.warn(
          "sync_event_hydration_backfill",
          "status" -> "record_failed",
          "stream_name" -> candidate.streamName,
          "error_type" -> error.getClass.getSimpleName
        ))
    }

  private def resolveWithRetry(payloadRef: String, attempt: Int): IO[String] =
    dbSemaphore.permit.use { _ =>
      IO.blocking(payloadResolver.resolvePayload(payloadRef))
    }.handleErrorWith {
      case _: TidbPayloadReadException if attempt < 3 =>
        IO.sleep((25L * (1L << (attempt - 1))).millis) *>
          resolveWithRetry(payloadRef, attempt + 1)
      case error => IO.raiseError(error)
    }

object SyncEventHydrationService:
  private val log = StructuredLogger(getClass)
