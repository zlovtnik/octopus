package com.sslproxy.coordinator.persistence

import com.sslproxy.coordinator.cutover.CutoffKey
import com.sslproxy.coordinator.domain.{BrokerRecordMetadata, IngestionDecision, ResolvedScanRequestRecord, ScanRequestRecord}
import com.sslproxy.coordinator.archive.ArchiveReceipt
import com.sslproxy.coordinator.processor.{Lease, ProcessorId, ProcessorRunStatus, ProcessorStatus}
import com.sslproxy.coordinator.tidb.{
  ArchiveCandidate,
  OutboxFailureDisposition,
  OutboxRecord,
  SyncEventHydrationCandidate,
  TidbLoad,
  TidbResult,
  WirelessBacklogEntry
}
import io.circe.Json

import java.time.Instant

trait IngestionStore[F[_]]:
  def pendingCount: DbResultT[F, Long]
  def processPending(
    streamNames: List[String],
    maxAttempts: Int,
    retryBackoffSeconds: Int,
    limit: Int
  ): DbResultT[F, Long]
  def prepareLoadDispatch(
    streamNames: List[String],
    batchMaxAttempts: Int,
    limit: Int
  ): DbResultT[F, Int]
  def loadConsumerOffsets(groupId: String, topic: String): DbResultT[F, Set[CutoffKey]]
  def recordScanRequests(records: List[ResolvedScanRequestRecord]): DbResultT[F, Int]
  def recordScanRequestWithEvidence(
    record: ResolvedScanRequestRecord,
    metadata: BrokerRecordMetadata
  ): DbResultT[F, IngestionDecision]
  def recordSkippedScanRequest(
    record: ScanRequestRecord,
    metadata: BrokerRecordMetadata
  ): DbResultT[F, Unit]
  def findHydrationCandidates(
    after: Option[SyncEventHydrationCandidate],
    limit: Int
  ): DbResultT[F, List[SyncEventHydrationCandidate]]
  def hydrateExistingEvent(
    candidate: SyncEventHydrationCandidate,
    payloadJson: String
  ): DbResultT[F, Boolean]

trait OutboxStore[F[_]]:
  def claim(ownerId: String, destinations: List[String], leaseSeconds: Int): DbResultT[F, Option[OutboxRecord]]
  def acknowledge(record: OutboxRecord): DbResultT[F, Boolean]
  def fail(
    record: OutboxRecord,
    error: String,
    retryBaseSeconds: Int,
    retryMaxSeconds: Int
  ): DbResultT[F, OutboxFailureDisposition]
  def recoverExpired: DbResultT[F, Int]

trait ResultStore[F[_]]:
  def recordResultWithEvidence(
    result: TidbResult,
    metadata: BrokerRecordMetadata
  ): DbResultT[F, Unit]
  def recordLoadResultsWithEvidence(
    records: List[(TidbLoad, TidbResult, BrokerRecordMetadata)]
  ): DbResultT[F, Unit]
  def recordResultsWithEvidence(
    records: List[(TidbResult, BrokerRecordMetadata)]
  ): DbResultT[F, Unit]

trait WirelessStore[F[_]]:
  def saveBacklog(
    dedupeKey: String,
    streamName: String,
    payload: Json,
    failureStage: String
  ): DbResultT[F, Unit]
  def listPendingBacklog(limit: Int): DbResultT[F, List[WirelessBacklogEntry]]
  def markBacklogSynced(dedupeKey: String, streamName: String): DbResultT[F, Boolean]
  def pruneBacklog(before: Instant): DbResultT[F, Int]
  def lookupDeviceByMac(mac: String): DbResultT[F, Option[String]]
  def listAuthorizedNetworks: DbResultT[F, String]
  def flushProbeBatch(probesJson: String): DbResultT[F, Int]

trait ProjectionStore[F[_]]:
  def generateRfAlerts(limit: Int): DbResultT[F, List[String]]
  def normalizeWirelessFrames(limit: Int): DbResultT[F, Int]
  def projectWirelessInventory(limit: Int): DbResultT[F, Int]
  def buildSearchDocuments(limit: Int): DbResultT[F, Int]
  def prepareEmbeddingJobs(limit: Int, embeddingModel: String): DbResultT[F, Int]
  def projectBehavior(limit: Int): DbResultT[F, Int]
  def projectTiming(limit: Int): DbResultT[F, Int]
  def projectSequences(limit: Int): DbResultT[F, Int]
  def projectBaselines(limit: Int): DbResultT[F, Int]
  def projectSimilarities(
    limit: Int,
    eventDuplicateDistance: Double,
    behaviorSimilarityThreshold: Double,
    sequenceDistanceThreshold: Double
  ): DbResultT[F, Int]
  def projectClusterCandidates(limit: Int, minimumSimilarity: Double): DbResultT[F, Int]
  def projectApprovedIdentities(limit: Int): DbResultT[F, Int]
  def projectInfrastructureGraph(limit: Int): DbResultT[F, Int]
  def projectDnsThreats(limit: Int): DbResultT[F, Int]
  def projectRisk(limit: Int): DbResultT[F, Int]

trait MaintenanceStore[F[_]]:
  def findArchiveCandidates(hotDays: Int, limit: Int): DbResultT[F, List[ArchiveCandidate]]
  def recordArchive(candidate: ArchiveCandidate, receipt: ArchiveReceipt): DbResultT[F, Unit]
  def claimLease(
    resourceType: String,
    resourceId: String,
    ownerId: String,
    token: String,
    ttlSeconds: Int
  ): DbResultT[F, Option[Lease]]
  def releaseLease(resourceType: String, resourceId: String, lease: Lease): DbResultT[F, Int]
  def renewLease(
    resourceType: String,
    resourceId: String,
    lease: Lease,
    ttlSeconds: Int
  ): DbResultT[F, Int]
  def startRetentionRun(
    runId: String,
    policyName: String,
    targetTable: String,
    cutoff: Instant,
    lease: Lease
  ): DbResultT[F, Int]
  def finishRetentionRun(
    runId: String,
    status: String,
    rowsSelected: Long,
    rowsArchived: Long,
    rowsDeleted: Long,
    error: Option[String]
  ): DbResultT[F, Int]
  def retainArchivedEvents(
    retentionDays: Int,
    tombstoneDays: Int,
    limit: Int,
    resourceType: String,
    resourceId: String,
    lease: Lease
  ): DbResultT[F, (Long, Long)]
  def pruneExpiredTombstones(limit: Int): DbResultT[F, Int]
  def retainSearchDocuments(
    retentionDays: Int,
    limit: Int,
    resourceType: String,
    resourceId: String,
    lease: Lease
  ): DbResultT[F, (Long, Long)]
  def cleanupStaleWorkers(limit: Int): DbResultT[F, Int]
  def reconcileWirelessProjections(limit: Int): DbResultT[F, Int]

trait ProcessorStateStore[F[_]]:
  def load: DbResultT[F, Map[ProcessorId, ProcessorStatus]]
  def persist(id: ProcessorId, status: ProcessorStatus, observedAt: Instant): DbResultT[F, Unit]
  def startRun(id: ProcessorId, runId: String, startedAt: Instant): DbResultT[F, Unit]
  def finishRun(
    runId: String,
    status: ProcessorRunStatus,
    errorClass: Option[String],
    errorText: Option[String],
    finishedAt: Instant
  ): DbResultT[F, Unit]
