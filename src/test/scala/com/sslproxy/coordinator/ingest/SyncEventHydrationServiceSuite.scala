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

import java.nio.file.Files
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

  test("unreadable outbox payload is quarantined without failing the backfill"):
    IO.blocking(Files.createTempDirectory("octopus-hydration-quarantine-")).bracket { directory =>
      for
        quarantined <- Ref.of[IO, Vector[(String, String)]](Vector.empty)
        semaphore <- Semaphore[IO](1)
        candidate = SyncEventHydrationCandidate(
          "missing-payload",
          "proxy.events",
          Timestamp.from(Instant.parse("2026-08-03T12:00:00Z")),
          "outbox://missing.json",
          None
        )
        store = new QuarantiningHydrationStore(candidate, quarantined)
        service = new SyncEventHydrationService(
          store,
          new PostgresPayloadResolver(directory.toString),
          new CoordinatorMetrics(SimpleMeterRegistry()),
          pageSize = 10,
          failureThreshold = 2,
          semaphore
        )
        result <- service.runOnce.compile.drain.attempt
        recorded <- quarantined.get
      yield
        assert(result.isRight)
        assertEquals(recorded.map(_._1), Vector("missing-payload"))
        assert(recorded.headOption.exists(_._2.contains("outbox payload read failed")))
    }(directory => IO.blocking(Files.deleteIfExists(directory)).void)

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

    def quarantineHydrationCandidate(
      candidate: SyncEventHydrationCandidate,
      error: String
    ): DbResultT[IO, Unit] = unused

  private final class QuarantiningHydrationStore(
      candidate: SyncEventHydrationCandidate,
      quarantined: Ref[IO, Vector[(String, String)]]
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
      EitherT.rightT[IO, DatabaseError](after.fold(List(candidate))(_ => Nil))

    def hydrateExistingEvent(
      candidate: SyncEventHydrationCandidate,
      payloadJson: String
    ): DbResultT[IO, Boolean] = unused

    def quarantineHydrationCandidate(
      candidate: SyncEventHydrationCandidate,
      error: String
    ): DbResultT[IO, Unit] =
      EitherT.liftF(quarantined.update(_ :+ (candidate.dedupeKey -> error)))
