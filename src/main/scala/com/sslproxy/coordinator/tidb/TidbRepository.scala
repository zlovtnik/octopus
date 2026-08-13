package com.sslproxy.coordinator.tidb

import cats.effect.IO
import cats.effect.implicits.*
import cats.effect.std.Semaphore
import cats.syntax.all.*
import com.sslproxy.coordinator.cutover.CutoffKey
import com.sslproxy.coordinator.archive.ArchiveReceipt
import com.sslproxy.coordinator.domain.{BrokerRecordMetadata, DatabaseError, IngestionDecision, IngestionDisposition, ResolvedScanRequestRecord, ScanRequestRecord}
import doobie.*
import doobie.implicits.*
import io.circe.{Json, parser as circeParser}
import com.sslproxy.coordinator.observability.{CoordinatorTracing, StructuredLogger}
import com.sslproxy.coordinator.processor.{IntelligencePreparation, Lease, SearchDocumentPreparation}
import com.sslproxy.coordinator.util.Sha256Utils
import com.sslproxy.coordinator.tidb.sql.{IdentityGraphSql, IngestionSql, IntelligenceSql, JobBatchSql, MaintenanceSql, OutboxSql, ProjectionSql, ResultSql, SearchPreparationSql, ThreatRiskSql, WirelessProcessorSql, WirelessProjectionSql, WirelessSql}
import com.sslproxy.coordinator.tidb.HydrationCursor

import java.nio.charset.StandardCharsets
import java.util.UUID
import io.opentelemetry.api.trace.SpanKind
import scala.concurrent.duration.*

class TidbRepository(xa: Transactor[IO],
  dbSemaphore: Option[Semaphore[IO]] = None):
  import TidbRepository.{log, stableUuid}

  def checkConnectivity(): IO[Either[DatabaseError, Unit]] =
    runDb("tidb.check_connectivity") {
      IngestionSql.ConnectivityQuery.unique.void
    }

  def pendingLedgerCount(): IO[Either[DatabaseError, Long]] =
    runDb("tidb.pending_ledger_count") {
      IngestionSql.PendingLedgerCountQuery.unique
    }

  def loadConsumerOffsets(
      groupId: String,
      topic: String
  ): IO[Either[DatabaseError, Set[CutoffKey]]] =
    runDb("tidb.load_consumer_offsets") {
      IngestionSql.consumerPartitions(groupId, topic).to[List].map { partitions =>
          partitions.map(p => CutoffKey(groupId, topic, p)).toSet
      }
    }

  def processIngestLedger(
      streamNames: List[String],
      scanMaxAttempts: Int,
      scanRetryBackoffSeconds: Int,
      ingestBatchSize: Int
  ): IO[Either[DatabaseError, Long]] =
    runDb("tidb.process_ingest_ledger") {
      JobBatchSql.processIngestLedger(
        streamNames,
        scanMaxAttempts,
        scanRetryBackoffSeconds,
        ingestBatchSize
      )
    }

  def prepareLoadDispatch(
      streamNames: List[String],
      maxAttempts: Int,
      limit: Int
  ): IO[Either[DatabaseError, Int]] =
    runDb("tidb.prepare_load_dispatch") {
      JobBatchSql.prepareLoadDispatch(streamNames, maxAttempts, limit)
        .fold(0.pure[ConnectionIO])(_.run)
    }

  def recordScanRequests(records: List[ResolvedScanRequestRecord]): IO[Either[DatabaseError, Int]] =
    if records.isEmpty then IO.pure(Right(0))
    else
      runDb("tidb.record_scan_request") {
        records.distinctBy(record => record.streamName -> record.dedupeKey)
          .traverse(record => ingestScanRequest(record, None))
          .map(_.count(_.disposition == IngestionDisposition.Processed))
      }

  def recordScanRequestWithEvidence(
      record: ResolvedScanRequestRecord,
      metadata: BrokerRecordMetadata
  ): IO[Either[DatabaseError, IngestionDecision]] =
    runDb("tidb.record_scan_request_with_evidence") {
      ingestScanRequest(record, Some(metadata))
    }

  def recordScanRequestsWithEvidence(
      records: List[(ResolvedScanRequestRecord, BrokerRecordMetadata)]
  ): IO[Either[DatabaseError, List[IngestionDecision]]] =
    if records.isEmpty then IO.pure(Right(Nil))
    else
      runDb("tidb.record_scan_requests_with_evidence") {
        records.traverse((record, metadata) => ingestScanRequest(record, Some(metadata)))
      }

  /** Persists durable bootstrap evidence for a broker record this consumer
    * deliberately skipped (stream filter), so the partition offset advance is
    * visible to loadConsumerOffsets across restarts.
    */
  def recordSkippedScanRequestEvidence(
      record: ScanRequestRecord,
      metadata: BrokerRecordMetadata
  ): IO[Either[DatabaseError, Unit]] =
    runDb("tidb.record_skipped_scan_request") {
      validateBrokerMetadata(metadata) *>
        existingBrokerEvidence(metadata).flatMap {
          case Some(existing) => verifyBrokerEvidence(metadata, record.dedupeKey, existing)
          case None =>
            persistBrokerEvidence(metadata, record.dedupeKey, IngestionDisposition.Rejected)
        }
    }

  def findSyncEventsNeedingHydration(
      after: Option[HydrationCursor],
      limit: Int
  ): IO[Either[DatabaseError, List[SyncEventHydrationCandidate]]] =
    runDb("tidb.find_sync_events_needing_hydration") {
      IngestionSql.hydrationCandidates(after, limit).to[List]
        .map(_.map(SyncEventHydrationCandidate.apply.tupled))
    }

  def hydrateExistingSyncEvent(
      candidate: SyncEventHydrationCandidate,
      payloadJson: String
  ): IO[Either[DatabaseError, Boolean]] =
    val parsed = circeParser.parse(payloadJson)
    val eventPayloadSha256 = Sha256Utils.sha256Hex(payloadJson.getBytes(StandardCharsets.UTF_8))
    val eventKind = parsed.toOption.flatMap { value =>
      value.hcursor.get[String]("event_type").toOption.filter(_.nonEmpty)
        .orElse(value.hcursor.get[String]("type").toOption.filter(_.nonEmpty))
    }

    runDb("tidb.hydrate_existing_sync_event") {
      parsed match
        case Left(error) => FC.raiseError(error)
        case Right(Json.Null) =>
          FC.raiseError(IllegalArgumentException("resolved backfill payload must not be JSON null"))
        case Right(_) if candidate.streamName == "wireless.audit" &&
            candidate.payloadJson.exists(_.nonEmpty) &&
            candidate.payloadSha256.contains(candidate.dedupeKey) =>
          hydrateWirelessProjection(candidate.dedupeKey, payloadJson).map(_ > 0)
        case Right(_) if candidate.streamName == "wireless.audit" &&
            candidate.dedupeKey != eventPayloadSha256 =>
          FC.raiseError(IllegalArgumentException(
            "wireless.audit backfill dedupe_key does not match the resolved event payload hash"
          ))
        case Right(_) =>
          hydrateSyncEvent(
            candidate.streamName,
            candidate.dedupeKey,
            payloadJson,
            eventPayloadSha256,
            eventKind
          )
    }

  private def ingestScanRequest(
      record: ResolvedScanRequestRecord,
      metadata: Option[BrokerRecordMetadata]
  ): ConnectionIO[IngestionDecision] =
    val payloadRef = record.payloadRef
    val eventKind = circeParser.parse(record.payloadJson).toOption.flatMap { value =>
      value.hcursor.get[String]("event_type").toOption.filter(_.nonEmpty)
        .orElse(value.hcursor.get[String]("type").toOption.filter(_.nonEmpty))
    }
    val observedAt = Option(record.observedAt).filter(_.nonEmpty).flatMap(parseTs)
    val jobId = stableUuid("job", record.streamName, record.dedupeKey)
    val batchId = stableUuid("batch", record.streamName, record.dedupeKey)

    def validate: ConnectionIO[Unit] =
      if record.streamName.isBlank then FC.raiseError(IllegalArgumentException("scan request stream_name must not be empty"))
      else if record.dedupeKey.isBlank then FC.raiseError(IllegalArgumentException("scan request dedupe_key must not be empty"))
      else if payloadRef.isBlank then FC.raiseError(IllegalArgumentException("scan request payload_ref must not be empty"))
      else if observedAt.isEmpty then FC.raiseError(IllegalArgumentException("scan request observed_at must be RFC3339"))
      else if metadata.exists(_.payloadSha256 != record.sourceRecordSha256) then
        FC.raiseError(IllegalArgumentException("scan request raw payload hash does not match decoded payload hash"))
      else if record.streamName == "wireless.audit" && record.dedupeKey != record.eventPayloadSha256 then
        FC.raiseError(IllegalArgumentException("wireless.audit dedupe_key must match the resolved event payload hash"))
      else ().pure[ConnectionIO]

    def existingEvidence(meta: BrokerRecordMetadata): ConnectionIO[Option[(String, String, String)]] =
      IngestionSql.existingEvidence(meta).option

    def verifyExisting(meta: BrokerRecordMetadata, existing: (String, String, String)): ConnectionIO[Unit] =
      val (payloadSha, cutoverSha, dedupeKey) = existing
      if payloadSha != record.sourceRecordSha256 || cutoverSha != meta.artifactSha256 || dedupeKey != record.dedupeKey then
        FC.raiseError(IllegalStateException(
          s"broker coordinate ${meta.topic}/${meta.partition}/${meta.offset}/${meta.consumerGroup} changed after ingestion"
        ))
      else ().pure[ConnectionIO]

    def persistEvidence(meta: BrokerRecordMetadata, disposition: IngestionDisposition): ConnectionIO[Unit] =
      for
        _ <- IngestionSql.persistEvidence(
          meta,
          record.sourceRecordSha256,
          record.dedupeKey,
          disposition
        ).run
        stored <- existingEvidence(meta).flatMap(_.fold(
          FC.raiseError[(String, String, String)](
            IllegalStateException("ingestion evidence disappeared after upsert")
          )
        )(_.pure[ConnectionIO]))
        _ <- verifyExisting(meta, stored)
        _ <- advanceConsumerOffset(meta)
      yield ()

    def decisionWithPersistedIds(
        disposition: IngestionDisposition
    ): ConnectionIO[IngestionDecision] =
      IngestionSql.jobBatchIds(record.streamName, record.dedupeKey).option.map {
        case Some((persistedJobId, persistedBatchId)) =>
          IngestionDecision(disposition, record.dedupeKey, persistedJobId, persistedBatchId)
        case None =>
          IngestionDecision(disposition, record.dedupeKey, jobId, batchId)
      }

    def createState: ConnectionIO[IngestionDecision] =
      for
        tombstoned <- IngestionSql.activeTombstone(record.streamName, record.dedupeKey).unique.map(_ == 1)
        decision <-
          if tombstoned then
            IngestionDecision(IngestionDisposition.Deduplicated, record.dedupeKey, jobId, batchId).pure[ConnectionIO]
          else
            for
              existed <- IngestionSql.syncEventExists(record.streamName, record.dedupeKey).unique.map(_ == 1)
              _ <- IngestionSql.insertSyncEvent(record, observedAt.orNull, eventKind).run
              _ <- hydrateSyncEvent(record, eventKind)
              _ <- IngestionSql.insertJob(jobId, record.streamName, record.dedupeKey).run
              persistedJobId <- IngestionSql.jobId(record.streamName, record.dedupeKey).unique
              cursor <- IngestionSql.cursor(record.streamName).option.map(_.getOrElse("0"))
              cursorEnd = if record.streamName == "wireless.audit" then
                observedAt.fold(record.dedupeKey)(_.toInstant.getEpochSecond.toString)
              else record.dedupeKey
              _ <- IngestionSql.insertBatch(batchId, persistedJobId, record, cursor, cursorEnd).run
              disposition = if existed then IngestionDisposition.Deduplicated else IngestionDisposition.Processed
              decision <- decisionWithPersistedIds(disposition)
            yield decision
      yield decision

    validate *> (metadata match
      case None => createState
      case Some(meta) =>
        existingEvidence(meta).flatMap {
          case Some(existing) =>
            verifyExisting(meta, existing) *>
              hydrateSyncEvent(record, eventKind) *>
              decisionWithPersistedIds(IngestionDisposition.Deduplicated)
          case None =>
            createState.flatTap(decision => persistEvidence(meta, decision.disposition))
        })

  private def hydrateSyncEvent(
      record: ResolvedScanRequestRecord,
      eventKind: Option[String]
  ): ConnectionIO[Boolean] =
    hydrateSyncEvent(
      record.streamName,
      record.dedupeKey,
      record.payloadJson,
      record.eventPayloadSha256,
      eventKind
    )

  private def hydrateSyncEvent(
      stream: String,
      key: String,
      payloadJson: String,
      eventPayloadSha256: String,
      eventKind: Option[String]
  ): ConnectionIO[Boolean] =
    circeParser.parse(payloadJson) match
      case Left(error) => FC.raiseError(error)
      case Right(Json.Null) =>
        FC.raiseError(IllegalArgumentException("resolved scan event payload must not be JSON null"))
      case Right(_) =>
        for
          updated <- IngestionSql.hydrateEvent(
            stream,
            key,
            payloadJson,
            eventPayloadSha256,
            eventKind
          ).run
          _ <- if updated > 0 && stream == "wireless.audit" then
            hydrateWirelessProjection(key, payloadJson)
          else 0.pure[ConnectionIO]
        yield updated > 0

  private def hydrateWirelessProjection(
      dedupeKey: String,
      payloadJson: String
  ): ConnectionIO[Int] =
    WirelessProjectionSql.hydrate(dedupeKey, payloadJson)
  def claimOutbox(
      ownerId: String,
      destinationTopics: List[String],
      leaseSeconds: Int
  ): IO[Either[DatabaseError, Option[OutboxRecord]]] =
    runDb("tidb.claim_outbox") {
      OutboxSql.claim(ownerId, destinationTopics, leaseSeconds, UUID.randomUUID().toString)
    }

  def acknowledgeOutbox(record: OutboxRecord): IO[Either[DatabaseError, Boolean]] =
    runDb("tidb.acknowledge_outbox") {
      validatedLoadBatchId(record).attempt.flatMap {
        case Right(loadBatchId) => acknowledgeValidatedOutbox(record, loadBatchId)
        case Left(error: IllegalArgumentException) => parkMalformedLoadOutbox(record, error)
        case Left(error) => FC.raiseError(error)
      }
    }

  private def acknowledgeValidatedOutbox(
      record: OutboxRecord,
      loadBatchId: Option[String]
  ): ConnectionIO[Boolean] =
    OutboxSql.acknowledge(record, loadBatchId)

  private def parkMalformedLoadOutbox(
      record: OutboxRecord,
      error: IllegalArgumentException
  ): ConnectionIO[Boolean] =
    val errorText = s"published load outbox payload was malformed: ${error.getMessage}"
    OutboxSql.parkMalformed(record, errorText)

  private def validatedLoadBatchId(record: OutboxRecord): ConnectionIO[Option[String]] =
    if record.destinationTopic != "sync.oracle.load" then none[String].pure[ConnectionIO]
    else
      val parsed =
        circeParser.parse(record.payload)
          .leftMap(error => IllegalArgumentException("load outbox payload must be valid JSON", error))
          .flatMap(
            _.hcursor.get[String]("batch_id")
              .leftMap(error => IllegalArgumentException("load outbox payload must contain a string batch_id", error))
          )
          .flatMap { value =>
            Either.catchNonFatal(UUID.fromString(value))
              .leftMap(error => IllegalArgumentException("load outbox batch_id must be a UUID", error))
              .filterOrElse(
                _.toString.equalsIgnoreCase(value),
                IllegalArgumentException("load outbox batch_id must use canonical UUID format")
              )
              .map(_.toString)
          }

      parsed.fold(FC.raiseError, batchId => batchId.some.pure[ConnectionIO])

  def failOutbox(
      record: OutboxRecord,
      errorText: String,
      retryBaseSeconds: Int,
      retryMaxSeconds: Int
  ): IO[Either[DatabaseError, OutboxFailureDisposition]] =
    runDb("tidb.fail_outbox") {
      val parked = record.attemptCount >= record.maxAttempts
      val disposition = if parked then OutboxFailureDisposition.Parked else OutboxFailureDisposition.RetryScheduled
      val status = if parked then "failed" else "pending"
      val delaySeconds = LeaseSql.retryDelaySeconds(record.attemptCount, retryBaseSeconds, retryMaxSeconds)

      OutboxSql.fail(record, status, errorText, delaySeconds).as(disposition)
    }

  def enqueueResult(result: TidbResult, attempt: Int): IO[Either[DatabaseError, Unit]] =
    runDb("tidb.enqueue_result") {
      enqueueResultTx(result, attempt)
    }

  def recordLoadResultWithEvidence(
      load: TidbLoad,
      result: TidbResult,
      metadata: BrokerRecordMetadata
  ): IO[Either[DatabaseError, Unit]] =
    runDb("tidb.record_load_result_with_evidence") {
      recordLoadResultWithEvidenceTx(load, result, metadata)
    }

  def recordLoadResultsWithEvidence(
      records: List[(TidbLoad, TidbResult, BrokerRecordMetadata)]
  ): IO[Either[DatabaseError, Unit]] =
    if records.isEmpty then IO.pure(Right(()))
    else
      runDb("tidb.record_load_results_with_evidence") {
        records.traverse_((load, result, metadata) =>
          recordLoadResultWithEvidenceTx(load, result, metadata)
        )
      }

  def recordResultWithEvidence(
      result: TidbResult,
      metadata: BrokerRecordMetadata
  ): IO[Either[DatabaseError, Unit]] =
    runDb("tidb.record_result_with_evidence") {
      recordResultWithEvidenceTx(result, metadata)
    }

  def recordResultsWithEvidence(
      records: List[(TidbResult, BrokerRecordMetadata)]
  ): IO[Either[DatabaseError, Unit]] =
    if records.isEmpty then IO.pure(Right(()))
    else
      runDb("tidb.record_results_with_evidence") {
        records.traverse_((result, metadata) => recordResultWithEvidenceTx(result, metadata))
      }

  private def recordLoadResultWithEvidenceTx(
      load: TidbLoad,
      result: TidbResult,
      metadata: BrokerRecordMetadata
  ): ConnectionIO[Unit] =
    val dedupeKey = s"load:${load.batchId}:${load.attempt.max(1)}"
    validateBrokerMetadata(metadata) *>
      existingBrokerEvidence(metadata).flatMap {
        case Some(existing) => verifyBrokerEvidence(metadata, dedupeKey, existing)
        case None =>
          enqueueResultTx(result, load.attempt) *>
            persistBrokerEvidence(metadata, dedupeKey, IngestionDisposition.Processed)
      }

  private def recordResultWithEvidenceTx(
      result: TidbResult,
      metadata: BrokerRecordMetadata
  ): ConnectionIO[Unit] =
    val attempt = metadata.messageKey.flatMap(resultAttempt).getOrElse(1)
    val dedupeKey = s"result:${result.batchId}:$attempt"

    validateBrokerMetadata(metadata) *>
      validateResult(result) *>
      existingBrokerEvidence(metadata).flatMap {
        case Some(existing) => verifyBrokerEvidence(metadata, dedupeKey, existing)
        case None =>
          applyResultTransition(result) *>
            persistBrokerEvidence(metadata, dedupeKey, IngestionDisposition.Processed)
      }

  private def enqueueResultTx(result: TidbResult, attempt: Int): ConnectionIO[Unit] =
    val safeAttempt = attempt.max(1)
    val outboxId = stableUuid("outbox:sync.oracle.result", result.batchId, safeAttempt.toString)
    ResultSql.enqueue(result, safeAttempt, outboxId)

  private def applyResultTransition(result: TidbResult): ConnectionIO[Unit] =
    for
      batch <- ResultSql.batchForUpdate(result.batchId).option
      row <- batch match
        case Some(value) if value.jobId == result.jobId => value.pure[ConnectionIO]
        case Some(value) => FC.raiseError(IllegalStateException(
          s"result job ${result.jobId} does not own batch ${result.batchId}; expected ${value.jobId}"
        ))
        case None => FC.raiseError(IllegalStateException(s"unknown result batch ${result.batchId}"))
      _ <- if result.status == "success" then completeSuccessfulResult(result, row.streamName, row.cursorEnd)
           else completeFailedResult(result, row)
    yield ()

  private def completeSuccessfulResult(
      result: TidbResult,
      streamName: String,
      cursorEnd: String
  ): ConnectionIO[Unit] =
    ResultSql.completeSuccessful(result, streamName, cursorEnd)

  private def completeFailedResult(
      result: TidbResult,
      batch: ResultSql.BatchState
  ): ConnectionIO[Unit] =
    val retry = result.retryable && batch.attemptCount < batch.maxAttempts

    if retry then
      ResultSql.scheduleRetry(result, batch)
    else
      ResultSql.completeFailed(result, batch)

  private def existingBrokerEvidence(
      metadata: BrokerRecordMetadata
  ): ConnectionIO[Option[(String, String, String)]] =
    IngestionSql.existingEvidence(metadata).option

  private def verifyBrokerEvidence(
      metadata: BrokerRecordMetadata,
      dedupeKey: String,
      existing: (String, String, String)
  ): ConnectionIO[Unit] =
    val (payloadSha256, artifactSha256, persistedDedupeKey) = existing
    if payloadSha256 != metadata.payloadSha256 ||
        artifactSha256 != metadata.artifactSha256 ||
        persistedDedupeKey != dedupeKey
    then FC.raiseError(IllegalStateException(
      s"broker coordinate ${metadata.consumerGroup}/${metadata.topic}/${metadata.partition}/${metadata.offset} changed after ingestion"
    ))
    else ().pure[ConnectionIO]

  private def persistBrokerEvidence(
      metadata: BrokerRecordMetadata,
      dedupeKey: String,
      disposition: IngestionDisposition
  ): ConnectionIO[Unit] =
    for
      _ <- IngestionSql.persistEvidence(
        metadata,
        metadata.payloadSha256,
        dedupeKey,
        disposition
      ).run
      stored <- existingBrokerEvidence(metadata).flatMap(_.fold(
        FC.raiseError[(String, String, String)](
          IllegalStateException("broker ingestion evidence disappeared after upsert")
        )
      )(_.pure[ConnectionIO]))
      _ <- verifyBrokerEvidence(metadata, dedupeKey, stored)
      _ <- advanceConsumerOffset(metadata)
    yield ()

  private def advanceConsumerOffset(metadata: BrokerRecordMetadata): ConnectionIO[Unit] =
    IngestionSql.advanceConsumerOffset(metadata).run.void

  private def validateBrokerMetadata(metadata: BrokerRecordMetadata): ConnectionIO[Unit] =
    if metadata.topic.isBlank then FC.raiseError(IllegalArgumentException("broker topic must not be blank"))
    else if metadata.partition < 0 then FC.raiseError(IllegalArgumentException("broker partition must be non-negative"))
    else if metadata.offset < 0L then FC.raiseError(IllegalArgumentException("broker offset must be non-negative"))
    else if metadata.consumerGroup.isBlank then FC.raiseError(IllegalArgumentException("broker consumer group must not be blank"))
    else if metadata.groupVersion <= 0 then FC.raiseError(IllegalArgumentException("broker group version must be positive"))
    else if !metadata.artifactSha256.matches("^[0-9a-f]{64}$") then
      FC.raiseError(IllegalArgumentException("cutover artifact SHA-256 must be lowercase hexadecimal"))
    else if !metadata.payloadSha256.matches("^[0-9a-f]{64}$") then
      FC.raiseError(IllegalArgumentException("broker payload SHA-256 must be lowercase hexadecimal"))
    else ().pure[ConnectionIO]

  private def validateResult(result: TidbResult): ConnectionIO[Unit] =
    if result.jobId.isBlank then FC.raiseError(IllegalArgumentException("result job_id must not be blank"))
    else if result.batchId.isBlank then FC.raiseError(IllegalArgumentException("result batch_id must not be blank"))
    else if result.status != "success" && result.status != "failed" then
      FC.raiseError(IllegalArgumentException(s"unsupported result status ${result.status}"))
    else if result.rowCount < 0 then FC.raiseError(IllegalArgumentException("result row_count must be non-negative"))
    else ().pure[ConnectionIO]

  private def resultAttempt(messageKey: String): Option[Int] =
    messageKey.lastIndexOf(':') match
      case index if index >= 0 && index < messageKey.length - 1 =>
        messageKey.substring(index + 1).toIntOption.filter(_ > 0)
      case _ => None

  def recoverExpiredOutboxLeases(): IO[Either[DatabaseError, Int]] =
    runDb("tidb.recover_expired_outbox_leases") {
      OutboxSql.RecoverExpiredLeases
    }

  def ensureCursor(streamName: String): IO[Either[DatabaseError, String]] =
    runDb("tidb.ensure_cursor") {
      for
        _ <- IngestionSql.ensureCursor(streamName).run
        cursor <- IngestionSql.cursor(streamName).unique
      yield cursor
    }

  def ensureAllCursors(streamNames: List[String], dbSemaphore: Semaphore[IO]): IO[Either[DatabaseError, String]] =
    if streamNames.isEmpty then IO.pure(Right("ok"))
    else
      dbSemaphore.available.flatMap { available =>
        val parallelism = available.toInt.max(1)
        streamNames.parTraverseN(parallelism) { name =>
            runDb(s"tidb.ensure_cursor_$name") {
              IngestionSql.ensureCursor(name).run
          }
        }.map { results =>
          if results.exists(_.isLeft) then
            results.collectFirst { case Left(e) => Left(e) }.get
          else
            Right("ok")
        }
      }

  private val windowSecs = 60
  private val signalThreshold = -50
  private val presenceWindowSecs = 300

  def generateShadowAlerts(limit: Int): IO[Either[DatabaseError, List[String]]] =
    runDb("tidb.generate_shadow_alerts") {
      ProjectionSql.generateShadowAlerts(windowSecs, signalThreshold, presenceWindowSecs, limit)
    }

  def normalizeWirelessFrames(limit: Int): IO[Either[DatabaseError, Int]] =
    runDb("tidb.normalize_wireless_frames") {
      WirelessProcessorSql.normalize(limit)
    }

  def projectWirelessInventory(limit: Int): IO[Either[DatabaseError, Int]] =
    runDb("tidb.project_wireless_inventory") {
      WirelessProcessorSql.projectInventory(limit)
    }

  def buildSearchDocuments(limit: Int): IO[Either[DatabaseError, Int]] =
    runDb("tidb.build_search_documents") {
      SearchPreparationSql.supportedKinds.traverse { kind =>
        SearchPreparationSql.candidates(kind, limit).to[List].flatMap { sources =>
          sources.traverse_ { source =>
            SearchDocumentPreparation.prepare(source).fold(
              error => FC.raiseError[Unit](IllegalArgumentException(error)),
              document => SearchPreparationSql.persist(document)
            )
          }.as(sources.size)
        }
      }.map(_.sum)
    }

  def prepareEmbeddingJobs(
      limit: Int,
      embeddingModel: String
  ): IO[Either[DatabaseError, Int]] =
    runDb("tidb.prepare_embedding_jobs") {
      SearchPreparationSql.supportedKinds.traverse { kind =>
        SearchPreparationSql.documentsMissingEmbeddingJobs(kind, embeddingModel, limit).to[List].flatMap { documents =>
          documents.traverse_ { case (documentId, checksum) =>
            val jobId = stableUuid("embedding", documentId, kind.embeddingKind, embeddingModel, checksum)
            SearchPreparationSql.enqueueEmbeddingJob(kind, jobId, documentId, checksum, embeddingModel)
          }.as(documents.size)
        }
      }.map(_.sum)
    }

  def projectBehavior(limit: Int): IO[Either[DatabaseError, Int]] =
    runDb("tidb.project_behavior") {
      IntelligenceSql.behaviorCandidates(limit).to[List].flatMap { frames =>
        IntelligencePreparation.behavior(frames).traverse(IntelligenceSql.persistBehavior).map(_.sum)
      }
    }

  def projectTiming(limit: Int): IO[Either[DatabaseError, Int]] =
    runDb("tidb.project_timing") {
      IntelligenceSql.timingCandidates(limit).to[List].flatMap { frames =>
        IntelligencePreparation.timing(frames).traverse(IntelligenceSql.persistTiming).map(_.sum)
      }
    }

  def projectSequences(limit: Int): IO[Either[DatabaseError, Int]] =
    runDb("tidb.project_sequences") {
      IntelligenceSql.sequenceCandidates(limit).to[List].flatMap { frames =>
        IntelligencePreparation.sequences(frames).traverse(IntelligenceSql.persistSequence).map(_.sum)
      }
    }

  def projectBaselines(limit: Int): IO[Either[DatabaseError, Int]] =
    runDb("tidb.project_baselines") {
      IntelligenceSql.baselineCandidates(limit).to[List].flatMap { rows =>
        rows.groupMap(_._1)(_._2).toList
          .traverse { case (bssid, signals) =>
            IntelligencePreparation.baseline(bssid, signals.toVector)
              .fold(0.pure[ConnectionIO])(IntelligenceSql.persistBaseline)
          }
          .map(_.sum)
      }
    }

  def projectSimilarities(
      limit: Int,
      eventDuplicateDistance: Double,
      behaviorSimilarityThreshold: Double,
      sequenceDistanceThreshold: Double
  ): IO[Either[DatabaseError, Int]] =
    runDb("tidb.project_similarities") {
      val candidates = List(
        IntelligenceSql.VectorKind.Event -> eventDuplicateDistance,
        IntelligenceSql.VectorKind.Behaviour -> (1.0d - behaviorSimilarityThreshold),
        IntelligenceSql.VectorKind.Sequence -> sequenceDistanceThreshold
      )
      candidates.traverse { case (kind, distance) =>
        IntelligenceSql.annReady(kind).unique.flatMap {
            case false => 0.pure[ConnectionIO]
            case true =>
              IntelligenceSql.similarityAnchors(kind, limit).to[List].flatMap { anchors =>
                anchors
                  .foldM((0, limit.max(1))) { case ((written, remaining), (_, documentId, model, embedding)) =>
                    if remaining <= 0 then (written, remaining).pure[ConnectionIO]
                    else
                      IntelligenceSql
                        .similarityCandidatesForAnchor(kind, documentId, model, embedding, distance, remaining).to[List].flatMap { values =>
          values.traverse { candidate =>
            IntelligencePreparation.similarity(candidate).fold(
              error => FC.raiseError[Int](IllegalArgumentException(error)),
              IntelligenceSql.persistSimilarity
            )
          }.map(counts => (written + counts.sum, remaining - values.size))
                        }
                  }
                  .map(_._1)
              }
        }
      }.map(_.sum)
    }

  def projectClusterCandidates(
      limit: Int,
      minimumSimilarity: Double
  ): IO[Either[DatabaseError, Int]] =
    runDb("tidb.project_cluster_candidates") {
      IdentityGraphSql.similarityEdges(minimumSimilarity, limit).to[List].flatMap { edges =>
        edges.distinctBy((left, right, _) => Vector(left, right).sorted).traverse {
          case (left, right, confidence) =>
            IdentityGraphSql.persistMergeCandidate(left, right, confidence)
        }.map(_.sum)
      }
    }

  def projectApprovedIdentities(limit: Int): IO[Either[DatabaseError, Int]] =
    runDb("tidb.project_approved_identities") {
      IdentityGraphSql.approvedIdentityEdges(limit).to[List].flatMap { edges =>
        IntelligencePreparation.identityClusters(edges).toList
          .traverse(IdentityGraphSql.persistCluster)
          .map(_.sum)
      }
    }

  def projectInfrastructureGraph(limit: Int): IO[Either[DatabaseError, Int]] =
    runDb("tidb.project_infrastructure_graph") {
      val runId = java.util.UUID.randomUUID().toString
      IdentityGraphSql.projectGraph(limit, runId)
    }

  def projectDnsThreats(limit: Int): IO[Either[DatabaseError, Int]] =
    runDb("tidb.project_dns_threats") {
      ThreatRiskSql.dnsCandidates(limit).to[List].flatMap { candidates =>
        candidates.traverse(candidate =>
          ThreatRiskSql.persistDnsThreat(IntelligencePreparation.dnsThreat(candidate))
        ).map(_.sum)
      }
    }

  def projectRisk(limit: Int): IO[Either[DatabaseError, Int]] =
    runDb("tidb.project_risk") {
      ThreatRiskSql.apRiskCandidates(limit).to[List].flatMap { candidates =>
        candidates.traverse { case (bssid, deauth, signal, typosquat, vendor, outlier) =>IntelligencePreparation.apRisk(
            bssid,
            deauth,
            signal,
            typosquat,
            vendor,
            outlier
          )
              .fold(
                error => FC.raiseError[Int](IllegalArgumentException(error)),
                ThreatRiskSql.persistApRisk)
        }.map(_.sum)
      }
    }

  def findArchiveCandidates(
      hotDays: Int,
      limit: Int
  ): IO[Either[DatabaseError, List[ArchiveCandidate]]] =
    runDb("tidb.find_archive_candidates") {
      MaintenanceSql.archiveCandidates(hotDays, limit).to[List]
    }

  def recordArchive(
      candidate: ArchiveCandidate,
      receipt: ArchiveReceipt
  ): IO[Either[DatabaseError, Unit]] =
    runDb("tidb.record_archive") {
      MaintenanceSql.recordArchive(candidate, receipt)
    }

  def claimMaintenanceLease(
      resourceType: String,
      resourceId: String,
      ownerId: String,
      token: String,
      ttlSeconds: Int
  ): IO[Either[DatabaseError, Option[Lease]]] =
    runDb("tidb.claim_maintenance_lease") {
      MaintenanceSql.claimLease(resourceType, resourceId, ownerId, token, ttlSeconds)
    }

  def releaseMaintenanceLease(
      resourceType: String,
      resourceId: String,
      lease: Lease
  ): IO[Either[DatabaseError, Int]] =
    runDb("tidb.release_maintenance_lease") {
      MaintenanceSql.releaseLease(resourceType, resourceId, lease).run
    }

  def renewMaintenanceLease(
      resourceType: String,
      resourceId: String,
      lease: Lease,
      ttlSeconds: Int
  ): IO[Either[DatabaseError, Int]] =
    runDb("tidb.renew_maintenance_lease") {
      MaintenanceSql.renewLease(resourceType, resourceId, lease, ttlSeconds).run
    }

  def startRetentionRun(
      runId: String,
      policyName: String,
      targetTable: String,
      cutoff: java.time.Instant,
      lease: Lease
  ): IO[Either[DatabaseError, Int]] =
    runDb("tidb.start_retention_run") {
      MaintenanceSql.startRetentionRun(runId, policyName, targetTable, cutoff, lease).run
    }

  def finishRetentionRun(
      runId: String,
      status: String,
      rowsSelected: Long,
      rowsArchived: Long,
      rowsDeleted: Long,
      error: Option[String]
  ): IO[Either[DatabaseError, Int]] =
    runDb("tidb.finish_retention_run") {
      MaintenanceSql.finishRetentionRun(
        runId,
        status,
        rowsSelected,
        rowsArchived,
        rowsDeleted,
        error
      ).run
    }

  def retainArchivedEvents(
      retentionDays: Int,
      tombstoneDays: Int,
      limit: Int,
      resourceType: String,
      resourceId: String,
      lease: Lease
  ): IO[Either[DatabaseError, (Long, Long)]] =
    runDb("tidb.retain_archived_events") {
      MaintenanceSql.retentionCandidates(retentionDays, limit).to[List].flatMap { candidates =>
        candidates.traverse(candidate =>
          MaintenanceSql.deleteRetainedEvent(
            candidate,
            tombstoneDays,
            resourceType,
            resourceId,
            lease
          )
        ).map(deleted => candidates.size.toLong -> deleted.count(identity).toLong)
      }
    }

  def pruneExpiredTombstones(limit: Int): IO[Either[DatabaseError, Int]] =
    runDb("tidb.prune_expired_tombstones") {
      MaintenanceSql.pruneTombstones(limit).run
    }

  def retainSearchDocuments(
      retentionDays: Int,
      limit: Int,
      resourceType: String,
      resourceId: String,
      lease: Lease
  ): IO[Either[DatabaseError, (Long, Long)]] =
    runDb("tidb.retain_search_documents") {
      MaintenanceSql.searchRetentionCandidates(retentionDays, limit).to[List].flatMap { candidates =>
        candidates.traverse(documentId =>
          MaintenanceSql.deleteRetainedSearchDocument(
            documentId,
            resourceType,
            resourceId,
            lease
          )
        ).map(deleted => candidates.size.toLong -> deleted.count(identity).toLong)
      }
    }

  def cleanupStaleWorkers(limit: Int): IO[Either[DatabaseError, Int]] =
    runDb("tidb.cleanup_stale_workers") {
      MaintenanceSql.cleanupStaleWorkers(limit)
    }

  def reconcileWirelessProjections(limit: Int): IO[Either[DatabaseError, Int]] =
    runDb("tidb.reconcile_wireless_projections") {
      for
        findings <- MaintenanceSql.reconcileMissingWirelessChildren(limit)
        _ <- WirelessProcessorSql.normalize(limit)
        resolved <- MaintenanceSql.ResolveWirelessFindings.run
      yield findings + resolved
    }

  def lookupDeviceByMac(mac: String): IO[Either[DatabaseError, Option[String]]] =
    runDb("tidb.lookup_device_by_mac") {
      WirelessSql.lookupDevice(mac).query[String].option
    }

  def listAuthorizedNetworks(): IO[Either[DatabaseError, String]] =
    runDb("tidb.list_authorized_networks") {
      WirelessSql.AuthorizedNetworksQuery.query[String].unique
    }

  def flushProbeBatch(probesJson: String): IO[Either[DatabaseError, Int]] =
    runDb("tidb.flush_probe_batch") {
      val parsed = circeParser.parse(probesJson).getOrElse(Json.Null)
      val probes = parsed.asArray.getOrElse(
        parsed.hcursor.downField("probes").as[Vector[Json]].getOrElse(Vector.empty)
      )

      if probes.isEmpty then 0.pure[ConnectionIO]
      else
        val batchId = java.security.MessageDigest.getInstance("MD5")
          .digest(probesJson.getBytes(java.nio.charset.StandardCharsets.UTF_8))
          .map("%02x".format(_)).mkString

        log.info("probe_flush_batch", "status" -> "parsed",
          "batch_id" -> batchId, "probe_count" -> probes.length.toString, "payload_bytes" -> probesJson.getBytes(java.nio.charset.StandardCharsets.UTF_8).length.toString)

        val validProbes = probes.flatMap { probe =>
          probe.hcursor.get[String]("client_mac").toOption
            .map(_.trim.toLowerCase(java.util.Locale.ROOT))
            .filter(TidbRepository.MacPattern.matches)
            .map(probe -> _)
        }
        val skipped = probes.size - validProbes.size
        if skipped > 0 then
          log.warn("probe_flush_batch", "status" -> "invalid_client_mac_skipped",
            "batch_id" -> batchId, "skipped_count" -> skipped.toString)

        validProbes.traverse { case (probe, clientMac) =>
          val ssid = probe.hcursor.get[String]("ssid").getOrElse("")
          val firstSeen = probe.hcursor.get[String]("first_seen").toOption.flatMap(parseTs)
          val lastSeen = probe.hcursor.get[String]("last_seen").toOption.flatMap(parseTs)
          val probeCount = probe.hcursor.get[Long]("probe_count").getOrElse(1L)
          val locationId = probe.hcursor.get[String]("location_id").toOption
          val observedBssid = probe.hcursor.get[String]("observed_bssid").toOption
            .orElse(probe.hcursor.get[String]("known_bssid").toOption)
            .orElse(probe.hcursor.get[String]("bssid").toOption)

          WirelessSql.upsertClientProbe(
            ssid,
            clientMac,
            observedBssid,
            firstSeen,
            lastSeen,
            probeCount,
            locationId,
            batchId
          ).update.run.map(affected => (1, affected))
        }.map { probeResults =>
          val logicalProbeCount = probeResults.map(_._1).sum
          val totalAffected = probeResults.map(_._2).sum
          log.info("probe_flush_batch", "status" -> "inserted",
            "batch_id" -> batchId, "total_affected_rows" -> totalAffected.toString)
          logicalProbeCount
        }
    }

  def saveWirelessBacklog(
      dedupeKey: String,
      streamName: String,
      payload: Json,
      failureStage: String
  ): IO[Either[DatabaseError, Unit]] =
    runDb("tidb.save_wireless_backlog") {
      WirelessSql.upsertBacklog(dedupeKey, streamName, payload, failureStage).update.run.void
    }

  def listPendingWirelessBacklog(limit: Int = 100): IO[Either[DatabaseError, List[WirelessBacklogEntry]]] =
    runDb("tidb.list_pending_wireless_backlog") {
      WirelessSql.oldestPending(limit.max(1).min(100)).query[
        (String, String, String, String, Int, java.sql.Timestamp)
      ].to[List].flatMap(_.traverse {
        case (dedupeKey, streamName, payload, stage, attempts, createdAt) =>
          circeParser.parse(payload) match
            case Right(json) =>
              WirelessBacklogEntry(
                dedupeKey,
                streamName,
                json,
                stage,
                attempts,
                createdAt.toInstant
              ).some.pure[ConnectionIO]
            case Left(error) =>
              FC.delay(log.warn(
                "wireless_backlog",
                "status" -> "invalid_payload",
                "dedupe_key" -> dedupeKey,
                "stream_name" -> streamName,
                "error" -> Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
              )) *>
                WirelessSql.markFailed(
                  dedupeKey,
                  streamName,
                  "failed",
                  "stored backlog payload is not valid JSON",
                  0L
                ).update.run.as(Option.empty[WirelessBacklogEntry])
      }).map(_.flatten)
    }

  def markWirelessBacklogSynced(
      dedupeKey: String,
      streamName: String
  ): IO[Either[DatabaseError, Boolean]] =
    runDb("tidb.mark_wireless_backlog_synced") {
      WirelessSql.markSynced(dedupeKey, streamName).update.run.map(_ > 0)
    }

  def pruneWirelessBacklog(
      cutoff: java.time.Instant,
      limit: Int = 1000
  ): IO[Either[DatabaseError, Int]] =
    runDb("tidb.prune_wireless_backlog") {
      WirelessSql.pruneSynced(java.sql.Timestamp.from(cutoff), limit.max(1).min(5000)).update.run
    }

  private def runDb[A](operation: String)(fa: ConnectionIO[A]): IO[Either[DatabaseError, A]] =
    val traced = CoordinatorTracing.span(
      operation,
      SpanKind.CLIENT,
      "db.system" -> "mysql",
      "db.namespace" -> "octopus_core",
      "db.operation.name" -> operation
    ) {
      TidbRepository.retryTransientWithPermit(operation, dbSemaphore)(fa.transact(xa))
    }
    traced.map(Right(_)).handleError { cause =>
      val sanitized = com.sslproxy.coordinator.util.ErrorSanitizer.message(cause)
      log.error("db_error", cause, "operation" -> operation)
      TidbErrorClass.classify(cause) match
        case TidbErrorClass.Retryable => Left(DatabaseError.Retryable(operation, cause, sanitized))
        case TidbErrorClass.Permanent => Left(DatabaseError.Permanent(operation, cause, sanitized))
    }

  private def parseTs(s: String): Option[java.sql.Timestamp] =
    try
      Some(java.sql.Timestamp.from(java.time.Instant.parse(s)))
    catch case _: Exception =>
      try
        Some(java.sql.Timestamp.valueOf(s.replace("T", " ").substring(0, 19)))
      catch case _: Exception => None

object TidbRepository:
  private val log = StructuredLogger(getClass)
  private val MacPattern = "(?i)^[0-9a-f]{2}(?::[0-9a-f]{2}){5}$".r
  private val transactionRetryMaxAttempts = 5
  private val transactionRetryBaseDelay = 25.millis

  private[tidb] def stableUuid(namespace: String, parts: String*): String =
    val values = s"$namespace:v2" +: parts.toVector
    val encoded = values.map { value =>
      s"${value.getBytes(StandardCharsets.UTF_8).length}:$value"
    }.mkString
    UUID.nameUUIDFromBytes(encoded.getBytes(StandardCharsets.UTF_8)).toString

  private[tidb] def retryTransient[A](operation: String)(fa: IO[A]): IO[A] =
    def loop(attempt: Int): IO[A] =
      fa.handleErrorWith { cause =>
        if attempt < transactionRetryMaxAttempts &&
            TidbErrorClass.classify(cause) == TidbErrorClass.Retryable
        then
          val baseDelay = transactionRetryBaseDelay * (1L << (attempt - 1))
          val jitterMs = scala.util.Random.nextLong(baseDelay.toMillis.max(1))
          val delay = baseDelay + jitterMs.millis
          IO(log.warn(
            "tidb_transaction_retry",
            "status" -> "retrying",
            "operation" -> operation,
            "attempt" -> s"$attempt/$transactionRetryMaxAttempts",
            "delay_ms" -> delay.toMillis.toString,
            "error" -> Option(cause.getMessage).getOrElse(cause.getClass.getSimpleName)
          )) *> IO.sleep(delay) *> loop(attempt + 1)
        else IO.raiseError(cause)
      }

    loop(1)

  private[tidb] def retryTransientWithPermit[A](
    operation: String,
    semaphore: Option[Semaphore[IO]]
  )(fa: IO[A]): IO[A] =
    retryTransient(operation) {
      semaphore.fold(fa)(_.permit.use(_ => fa))
    }
