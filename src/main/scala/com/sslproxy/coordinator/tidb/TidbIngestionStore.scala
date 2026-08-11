package com.sslproxy.coordinator.tidb

import cats.data.EitherT
import cats.effect.IO
import com.sslproxy.coordinator.cutover.CutoffKey
import com.sslproxy.coordinator.domain.{
  BrokerRecordMetadata,
  IngestionDecision,
  ResolvedScanRequestRecord,
  ScanRequestRecord
}
import com.sslproxy.coordinator.persistence.{DbResultT, IngestionStore}
import com.sslproxy.coordinator.tidb.HydrationCursor

final class TidbIngestionStore(repository: TidbRepository) extends IngestionStore[IO]:
  def pendingCount: DbResultT[IO, Long] =
    EitherT(repository.pendingLedgerCount())

  def processPending(
      streamNames: List[String],
      maxAttempts: Int,
      retryBackoffSeconds: Int,
      limit: Int
  ): DbResultT[IO, Long] =
    EitherT(repository.processIngestLedger(
      streamNames,
      maxAttempts,
      retryBackoffSeconds,
      limit
    ))

  def prepareLoadDispatch(
      streamNames: List[String],
      batchMaxAttempts: Int,
      limit: Int
  ): DbResultT[IO, Int] =
    EitherT(repository.prepareLoadDispatch(streamNames, batchMaxAttempts, limit))

  def loadConsumerOffsets(groupId: String, topic: String): DbResultT[IO, Set[CutoffKey]] =
    EitherT(repository.loadConsumerOffsets(groupId, topic))

  def recordScanRequests(records: List[ResolvedScanRequestRecord]): DbResultT[IO, Int] =
    EitherT(repository.recordScanRequests(records))

  def recordScanRequestWithEvidence(
      record: ResolvedScanRequestRecord,
      metadata: BrokerRecordMetadata
  ): DbResultT[IO, IngestionDecision] =
    EitherT(repository.recordScanRequestWithEvidence(record, metadata))

  def recordSkippedScanRequest(
      record: ScanRequestRecord,
      metadata: BrokerRecordMetadata
  ): DbResultT[IO, Unit] =
    EitherT(repository.recordSkippedScanRequestEvidence(record, metadata))

  def findHydrationCandidates(
      after: Option[HydrationCursor],
      limit: Int
  ): DbResultT[IO, List[SyncEventHydrationCandidate]] =
    EitherT(repository.findSyncEventsNeedingHydration(after, limit))

  def hydrateExistingEvent(
      candidate: SyncEventHydrationCandidate,
      payloadJson: String
  ): DbResultT[IO, Boolean] =
    EitherT(repository.hydrateExistingSyncEvent(candidate, payloadJson))
