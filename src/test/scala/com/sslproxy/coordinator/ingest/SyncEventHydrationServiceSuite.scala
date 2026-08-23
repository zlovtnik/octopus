package com.sslproxy.coordinator.ingest

import cats.data.EitherT
import cats.effect.{IO, Ref}
import cats.effect.std.Semaphore
import com.sslproxy.coordinator.domain.{
  BrokerRecordMetadata,
  DatabaseError,
  IngestionDecision,
  ResolvedScanRequestRecord
}
import com.sslproxy.coordinator.observability.CoordinatorMetrics
import com.sslproxy.coordinator.persistence.{DbResultT, IngestionStore}
import com.sslproxy.coordinator.postgres.{HydrationCursor, SyncEventHydrationCandidate, PostgresPayloadResolver}
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import munit.CatsEffectSuite

import java.sql.Timestamp
import java.time.Instant

class SyncEventHydrationServiceSuite extends CatsEffectSuite:
  test("backfill fails and stops after the consecutive failure threshold"):
    for
      calls <- Ref.of[IO, Int](0)
      semaphore <- Semaphore[IO](1)
      candidates = List.tabulate(3) { index =>
        SyncEventHydrationCandidate(
          s"dedupe-$index",
          "proxy.events",
          Timestamp.from(Instant.parse(s"2026-08-03T12:00:0${index}Z")),
          s"inline://json/$index",
          Some("{}")
        )
      }
      store = new FailingHydrationStore(candidates, calls)
      metrics = new CoordinatorMetrics(SimpleMeterRegistry())
      service = new SyncEventHydrationService(
        store,
        new PostgresPayloadResolver("/unused"),
        metrics,
        pageSize = 10,
        failureThreshold = 2,
        semaphore
      )
      result <- service.runOnce.compile.drain.attempt
      attempted <- calls.get
    yield
      assert(result.left.exists(_.getMessage.contains("failure threshold reached")))
      assertEquals(attempted, 2)

  private final class FailingHydrationStore(
    candidates: List[SyncEventHydrationCandidate],
    calls: Ref[IO, Int]
  ) extends IngestionStore[IO]:
    private def unused[A]: DbResultT[IO, A] =
      throw UnsupportedOperationException("unused test store operation")

    def pendingCount: DbResultT[IO, Long] = unused

    def processPending(
      streamNames: List[String],
      maxAttempts: Int,
      retryBackoffSeconds: Int,
      limit: Int
    ): DbResultT[IO, Long] = unused

    def prepareLoadDispatch(
      streamNames: List[String],
      batchMaxAttempts: Int,
      limit: Int
    ): DbResultT[IO, Int] = unused

    def recordScanRequests(
      records: List[ResolvedScanRequestRecord]
    ): DbResultT[IO, Int] = unused

    def recordScanRequestWithEvidence(
      record: ResolvedScanRequestRecord,
      metadata: BrokerRecordMetadata
    ): DbResultT[IO, IngestionDecision] = unused

    def findHydrationCandidates(
      after: Option[HydrationCursor],
      limit: Int
    ): DbResultT[IO, List[SyncEventHydrationCandidate]] =
      EitherT.rightT[IO, DatabaseError](candidates)

    def hydrateExistingEvent(
      candidate: SyncEventHydrationCandidate,
      payloadJson: String
    ): DbResultT[IO, Boolean] =
      EitherT(
        calls
          .update(_ + 1)
          .as(
            Left(
              DatabaseError.Permanent(
                "postgres.hydrate_existing_sync_event",
                IllegalStateException("invalid stored payload"),
                "invalid stored payload"
              )
            )
          )
      )
