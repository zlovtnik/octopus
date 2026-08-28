package com.sslproxy.coordinator.processor

import cats.data.EitherT
import cats.effect.{Deferred, IO, Ref}
import com.sslproxy.coordinator.archive.ArchiveReceipt
import com.sslproxy.coordinator.domain.DatabaseError
import com.sslproxy.coordinator.persistence.{DbResultT, MaintenanceStore}
import com.sslproxy.coordinator.postgres.ArchiveCandidate
import munit.CatsEffectSuite

import java.time.Instant
import scala.concurrent.duration.*

class FencedRetentionRunnerSuite extends CatsEffectSuite:
  test("successful fenced work records completion and releases its lease"):
    for
      store <- TestMaintenanceStore.create()
      runner = new FencedRetentionRunner[IO](
        store,
        ProcessorId.EventRetention,
        "worker-1",
        "events",
        "sync_events",
        30,
        60
      )
      _ <- runner.runOnce(_ => IO.pure(RetentionCounts(3L, 2L, 1L)))
      finishes <- store.finishes.get
      releases <- store.releases.get
    yield
      assertEquals(finishes.map(_._1), List("completed"))
      assertEquals(finishes.map(_._2), List(RetentionCounts(3L, 2L, 1L)))
      assertEquals(releases, 1)

  test("cancellation always releases the fenced lease"):
    for
      store <- TestMaintenanceStore.create()
      started <- Deferred[IO, Unit]
      runner = new FencedRetentionRunner[IO](
        store,
        ProcessorId.SearchRetention,
        "worker-1",
        "search",
        "atheros_search.search_documents",
        30,
        60
      )
      fiber <- runner.runOnce(_ => started.complete(()) *> IO.never).start
      _ <- started.get
      _ <- fiber.cancel
      releases <- store.releases.get
      finishes <- store.finishes.get
    yield
      assertEquals(releases, 1)
      assertEquals(finishes.map(_._1), List("cancelled"))

  test("long-running work renews its lease"):
    for
      store <- TestMaintenanceStore.create()
      runner = new FencedRetentionRunner[IO](
        store,
        ProcessorId.EventRetention,
        "worker-1",
        "events",
        "sync_events",
        30,
        3
      )
      _ <- runner.runOnce(_ => IO.sleep(1100.millis).as(RetentionCounts(1L, 1L, 1L)))
      renewals <- store.renewals.get
    yield assert(renewals >= 1)

  test("lease loss cancels work and records a failed run"):
    for
      store <- TestMaintenanceStore.create(renewedRows = 0)
      started <- Deferred[IO, Unit]
      runner = new FencedRetentionRunner[IO](
        store,
        ProcessorId.EventRetention,
        "worker-1",
        "events",
        "sync_events",
        30,
        1
      )
      result <- runner.runOnce(_ => started.complete(()) *> IO.never).attempt
      finishes <- store.finishes.get
      releases <- store.releases.get
    yield
      assert(result.swap.exists(_.getMessage.contains("affected 0 rows")))
      assertEquals(finishes.map(_._1), List("failed"))
      assertEquals(releases, 1)

  test("generic periodic work is renewed and released under its processor fence"):
    for
      store <- TestMaintenanceStore.create()
      runner = new FencedWorkRunner[IO](store, "worker-1")
      result <- runner.runOnce(ProcessorId.BehaviorProjector, 3.seconds) { lease =>
        IO.sleep(1100.millis).as(lease.fence)
      }
      renewals <- store.renewals.get
      releases <- store.releases.get
    yield
      assertEquals(result, Some(1L))
      assert(renewals >= 1)
      assertEquals(releases, 1)

  private final class TestMaintenanceStore(
      val finishes: Ref[IO, List[(String, RetentionCounts)]],
      val releases: Ref[IO, Int],
      val renewals: Ref[IO, Int],
      renewedRows: Int
  ) extends MaintenanceStore[IO]:
    private val lease = Lease(
      "maintenance/event-retention",
      "worker-1",
      "token",
      1L,
      Instant.now().plusSeconds(3600L)
    )

    def findArchiveCandidates(hotDays: Int, limit: Int): DbResultT[IO, List[ArchiveCandidate]] =
      right(List.empty)

    def recordArchive(candidate: ArchiveCandidate, receipt: ArchiveReceipt): DbResultT[IO, Unit] =
      right(())

    def quarantineArchiveCandidate(candidate: ArchiveCandidate, error: String): DbResultT[IO, Unit] =
      right(())

    def claimLease(
        resourceType: String,
        resourceId: String,
        ownerId: String,
        token: String,
        ttlSeconds: Int
    ): DbResultT[IO, Option[Lease]] = right(Some(lease.copy(ownerId = ownerId, token = token)))

    def releaseLease(resourceType: String, resourceId: String, lease: Lease): DbResultT[IO, Int] =
      EitherT.liftF(releases.update(_ + 1).as(1))

    def renewLease(
        resourceType: String,
        resourceId: String,
        lease: Lease,
        ttlSeconds: Int
    ): DbResultT[IO, Int] = EitherT.liftF(renewals.update(_ + 1).as(renewedRows))

    def startRetentionRun(
        runId: String,
        policyName: String,
        targetTable: String,
        cutoff: Instant,
        lease: Lease
    ): DbResultT[IO, Int] = right(1)

    def finishRetentionRun(
        runId: String,
        status: String,
        rowsSelected: Long,
        rowsArchived: Long,
        rowsDeleted: Long,
        error: Option[String]
    ): DbResultT[IO, Int] =
      EitherT.liftF(
        finishes.update(_ :+ (status -> RetentionCounts(rowsSelected, rowsArchived, rowsDeleted))).as(1)
      )

    def retainArchivedEvents(
        retentionDays: Int,
        tombstoneDays: Int,
        limit: Int,
        resourceType: String,
        resourceId: String,
        lease: Lease
    ): DbResultT[IO, (Long, Long)] = right(0L -> 0L)

    def pruneExpiredTombstones(limit: Int): DbResultT[IO, Int] = right(0)

    def retainSearchDocuments(
        retentionDays: Int,
        limit: Int,
        resourceType: String,
        resourceId: String,
        lease: Lease
    ): DbResultT[IO, (Long, Long)] = right(0L -> 0L)

    def cleanupStaleWorkers(limit: Int): DbResultT[IO, Int] = right(0)

    def reconcileWirelessProjections(limit: Int): DbResultT[IO, Int] = right(0)

    private def right[A](value: A): DbResultT[IO, A] =
      EitherT.rightT[IO, DatabaseError](value)

  private object TestMaintenanceStore:
    def create(renewedRows: Int = 1): IO[TestMaintenanceStore] =
      for
        finishes <- Ref.of[IO, List[(String, RetentionCounts)]](List.empty)
        releases <- Ref.of[IO, Int](0)
        renewals <- Ref.of[IO, Int](0)
      yield new TestMaintenanceStore(finishes, releases, renewals, renewedRows)
