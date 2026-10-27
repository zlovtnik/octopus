package com.sslproxy.coordinator.kafka

import cats.effect.{IO, Ref}
import com.sslproxy.coordinator.domain.{
  BrokerRecordMetadata,
  DatabaseError,
  IngestionDecision,
  IngestionDisposition,
  IngestionInvariantViolation,
  ResolvedScanRequestRecord,
  ScanRequestRecord
}
import com.sslproxy.coordinator.observability.CoordinatorMetrics
import com.sslproxy.coordinator.util.Sha256Utils
import munit.CatsEffectSuite

class DurableIngestionSuite extends CatsEffectSuite:
  private val raw = "{\"custom\":true}"
  private val hash = Sha256Utils.sha256Hex(raw)
  private val record = ResolvedScanRequestRecord(
    ScanRequestRecord(raw, hash, "custom.new-event", "event-1", "2026-10-25T12:00:00Z", "inline://event-1"),
    raw,
    hash
  )
  private val metadata = BrokerRecordMetadata("sync.scan.request", 0, 1L, "octopus-scan-v1", 1, hash, None, hash)
  private val decision = IngestionDecision(IngestionDisposition.Processed, "event-1", "", "")

  test("successful intake records processed and duplicate metrics without parking"):
    val metrics = CoordinatorMetrics.withClock(() => 0L)
    for
      parked <- Ref.of[IO, Int](0)
      _ <- DurableIngestion.persist(record, metadata, IO.pure(Right(decision)), metrics, _ => parked.update(_ + 1))
      _ <- DurableIngestion.persist(
        record,
        metadata,
        IO.pure(Right(decision.copy(disposition = IngestionDisposition.Deduplicated))),
        metrics,
        _ => parked.update(_ + 1)
      )
      count <- parked.get
    yield
      assertEquals(count, 0)
      assert(metrics.scrape.contains("coordinator_sync_events_hydrated_total 1.0"))
      assert(metrics.scrape.contains("coordinator_ingest_deduplicated_total 1.0"))

  test("permanent record errors wait for acknowledged parking"):
    val cause = IllegalArgumentException("invalid field")
    val error = DatabaseError.Permanent("ingest", cause, "invalid field")
    for
      parked <- Ref.of[IO, Option[Throwable]](None)
      _ <- DurableIngestion.persist(
        record,
        metadata,
        IO.pure(Left(error)),
        CoordinatorMetrics.withClock(() => 0L),
        error => parked.set(Some(error))
      )
      parkedError <- parked.get
    yield assertEquals(parkedError, Some(cause))

  test("failed poison publication fails durable handling instead of authorizing an offset commit"):
    val error = DatabaseError.Permanent("ingest", IllegalArgumentException("invalid field"), "invalid field")
    val failure = RuntimeException("broker unavailable")
    DurableIngestion
      .persist(record, metadata, IO.pure(Left(error)), CoordinatorMetrics.withClock(() => 0L), _ => IO.raiseError(failure))
      .attempt
      .map(result => assertEquals(result, Left(failure)))

  test("explicit retryable errors never park even when their cause has no SQL retry hint"):
    val error = DatabaseError.Retryable("ingest", RuntimeException("dependency unavailable"), "dependency unavailable")
    assert(!DurableIngestion.isParkable(error))
    assertFailsWithoutParking(error)

  test("nested contradictory evidence fails closed even when classified permanent"):
    val cause = RuntimeException("wrapped", IngestionInvariantViolation("broker coordinate changed"))
    val error = DatabaseError.Permanent("ingest", cause, "broker coordinate changed")
    assert(DurableIngestion.isInvariantViolation(error))
    assert(!DurableIngestion.isParkable(error))
    assertFailsWithoutParking(error)

  private def assertFailsWithoutParking(error: DatabaseError): IO[Unit] =
    for
      parked <- Ref.of[IO, Boolean](false)
      result <- DurableIngestion
        .persist(record, metadata, IO.pure(Left(error)), CoordinatorMetrics.withClock(() => 0L), _ => parked.set(true))
        .attempt
      wasParked <- parked.get
    yield
      assert(result.isLeft)
      assertEquals(wasParked, false)
