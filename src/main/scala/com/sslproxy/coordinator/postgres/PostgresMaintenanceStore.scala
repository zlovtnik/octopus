package com.sslproxy.coordinator.postgres

import cats.data.EitherT
import cats.effect.IO
import com.sslproxy.coordinator.archive.ArchiveReceipt
import com.sslproxy.coordinator.persistence.{DbResultT, MaintenanceStore}
import com.sslproxy.coordinator.processor.Lease

final class PostgresMaintenanceStore(repository: PostgresRepository) extends MaintenanceStore[IO]:
  def findArchiveCandidates(hotDays: Int, limit: Int): DbResultT[IO, List[ArchiveCandidate]] =
    EitherT(repository.findArchiveCandidates(hotDays, limit))

  def recordArchive(candidate: ArchiveCandidate, receipt: ArchiveReceipt): DbResultT[IO, Unit] =
    EitherT(repository.recordArchive(candidate, receipt))

  def claimLease(
      resourceType: String,
      resourceId: String,
      ownerId: String,
      token: String,
      ttlSeconds: Int
  ): DbResultT[IO, Option[Lease]] =
    EitherT(repository.claimMaintenanceLease(resourceType, resourceId, ownerId, token, ttlSeconds))

  def releaseLease(resourceType: String, resourceId: String, lease: Lease): DbResultT[IO, Int] =
    EitherT(repository.releaseMaintenanceLease(resourceType, resourceId, lease))

  def renewLease(
      resourceType: String,
      resourceId: String,
      lease: Lease,
      ttlSeconds: Int
  ): DbResultT[IO, Int] =
    EitherT(repository.renewMaintenanceLease(resourceType, resourceId, lease, ttlSeconds))

  def startRetentionRun(
      runId: String,
      policyName: String,
      targetTable: String,
      cutoff: java.time.Instant,
      lease: Lease
  ): DbResultT[IO, Int] =
    EitherT(repository.startRetentionRun(runId, policyName, targetTable, cutoff, lease))

  def finishRetentionRun(
      runId: String,
      status: String,
      rowsSelected: Long,
      rowsArchived: Long,
      rowsDeleted: Long,
      error: Option[String]
  ): DbResultT[IO, Int] =
    EitherT(repository.finishRetentionRun(
      runId,
      status,
      rowsSelected,
      rowsArchived,
      rowsDeleted,
      error
    ))

  def retainArchivedEvents(
      retentionDays: Int,
      tombstoneDays: Int,
      limit: Int,
      resourceType: String,
      resourceId: String,
      lease: Lease
  ): DbResultT[IO, (Long, Long)] =
    EitherT(repository.retainArchivedEvents(
      retentionDays,
      tombstoneDays,
      limit,
      resourceType,
      resourceId,
      lease
    ))

  def pruneExpiredTombstones(limit: Int): DbResultT[IO, Int] =
    EitherT(repository.pruneExpiredTombstones(limit))

  def retainSearchDocuments(
      retentionDays: Int,
      limit: Int,
      resourceType: String,
      resourceId: String,
      lease: Lease
  ): DbResultT[IO, (Long, Long)] =
    EitherT(repository.retainSearchDocuments(
      retentionDays,
      limit,
      resourceType,
      resourceId,
      lease
    ))

  def cleanupStaleWorkers(limit: Int): DbResultT[IO, Int] =
    EitherT(repository.cleanupStaleWorkers(limit))

  def reconcileWirelessProjections(limit: Int): DbResultT[IO, Int] =
    EitherT(repository.reconcileWirelessProjections(limit))
