package com.sslproxy.coordinator.tidb.sql

import cats.syntax.all.*
import com.sslproxy.coordinator.archive.ArchiveReceipt
import com.sslproxy.coordinator.processor.Lease
import com.sslproxy.coordinator.tidb.ArchiveCandidate
import doobie.{ConnectionIO, Query0, Update0}
import doobie.free.connection as FC
import doobie.implicits.*

import java.sql.Timestamp
import java.time.Instant

object MaintenanceSql:
  def archiveCandidates(hotDays: Int, limit: Int): Query0[ArchiveCandidate] =
    sql"""SELECT dedupe_key, stream_name, observed_at, CAST(payload AS CHAR), payload_sha256
           FROM sync_events
           WHERE stream_name = 'wireless.audit'
             AND payload_archived = 0
             AND payload IS NOT NULL
             AND payload_sha256 IS NOT NULL
             AND observed_at < TIMESTAMPADD(DAY, -${hotDays.max(1)}, CURRENT_TIMESTAMP(6))
           ORDER BY observed_at, dedupe_key
           LIMIT ${limit.max(1)}"""
      .query[(String, String, java.sql.Timestamp, String, String)]
      .map(ArchiveCandidate.apply.tupled)

  def recordArchive(candidate: ArchiveCandidate, receipt: ArchiveReceipt): ConnectionIO[Unit] =
    for
      _ <- sql"""INSERT INTO sync_event_payload_archives (
                   dedupe_key, stream_name, observed_at, payload_sha256,
                   archive_uri, payload_bytes, archived_at, created_at, updated_at
                 ) VALUES (
                   ${candidate.dedupeKey}, ${candidate.streamName}, ${candidate.observedAt},
                   ${receipt.payloadSha256}, ${receipt.uri}, ${receipt.payloadBytes},
                   CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6)
                 ) ON DUPLICATE KEY UPDATE
                   archive_uri = VALUES(archive_uri),
                   payload_bytes = VALUES(payload_bytes),
                   updated_at = CURRENT_TIMESTAMP(6)""".update.run
      updated <- sql"""UPDATE sync_events
                         SET payload_archive_uri = ${receipt.uri},
                             archived_payload_bytes = ${receipt.payloadBytes},
                             payload_archived_at = CURRENT_TIMESTAMP(6),
                             payload_archived = 1,
                             payload = NULL,
                             updated_at = CURRENT_TIMESTAMP(6)
                         WHERE dedupe_key = ${candidate.dedupeKey}
                           AND stream_name = ${candidate.streamName}
                           AND payload_archived = 0
                           AND payload_sha256 = ${receipt.payloadSha256}
                           AND payload IS NOT NULL""".update.run
      _ <- if updated == 1 then FC.unit
           else FC.raiseError(IllegalStateException(
             s"archive candidate ${candidate.streamName}/${candidate.dedupeKey} changed before metadata commit"
           ))
    yield ()

  def claimLease(
      resourceType: String,
      resourceId: String,
      ownerId: String,
      token: String,
      ttlSeconds: Int
  ): ConnectionIO[Option[Lease]] =
    for
      _ <- sql"""INSERT INTO work_leases (resource_type, resource_id)
                 VALUES ($resourceType, $resourceId)
                 ON DUPLICATE KEY UPDATE resource_id = VALUES(resource_id)""".update.run
      claimed <- sql"""UPDATE work_leases
                          SET owner_id = $ownerId,
                              lease_token = $token,
                              fence = fence + 1,
                              attempt_count = attempt_count + 1,
                              lease_expires_at = TIMESTAMPADD(SECOND, ${ttlSeconds.max(1)}, CURRENT_TIMESTAMP(6)),
                              last_error = NULL,
                              updated_at = CURRENT_TIMESTAMP(6)
                          WHERE resource_type = $resourceType
                            AND resource_id = $resourceId
                            AND (next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP(6))
                            AND (lease_expires_at IS NULL OR lease_expires_at <= CURRENT_TIMESTAMP(6))""".update.run
      lease <- if claimed != 1 then none[Lease].pure[ConnectionIO]
               else
                 sql"""SELECT fence, lease_expires_at
                        FROM work_leases
                        WHERE resource_type = $resourceType
                          AND resource_id = $resourceId
                          AND owner_id = $ownerId
                          AND lease_token = $token"""
                   .query[(Long, Timestamp)]
                   .option
                   .map(_.map((fence, expiresAt) =>
                     Lease(s"$resourceType/$resourceId", ownerId, token, fence, expiresAt.toInstant)
                   ))
    yield lease

  def releaseLease(resourceType: String, resourceId: String, lease: Lease): Update0 =
    sql"""UPDATE work_leases
           SET owner_id = NULL,
               lease_token = NULL,
               lease_expires_at = NULL,
               next_attempt_at = CURRENT_TIMESTAMP(6),
               updated_at = CURRENT_TIMESTAMP(6)
           WHERE resource_type = $resourceType
             AND resource_id = $resourceId
             AND owner_id = ${lease.ownerId}
             AND lease_token = ${lease.token}
             AND fence = ${lease.fence}""".update

  def renewLease(
      resourceType: String,
      resourceId: String,
      lease: Lease,
      ttlSeconds: Int
  ): Update0 =
    sql"""UPDATE work_leases
           SET lease_expires_at = TIMESTAMPADD(SECOND, ${ttlSeconds.max(1)}, CURRENT_TIMESTAMP(6)),
               updated_at = CURRENT_TIMESTAMP(6)
           WHERE resource_type = $resourceType
             AND resource_id = $resourceId
             AND owner_id = ${lease.ownerId}
             AND lease_token = ${lease.token}
             AND fence = ${lease.fence}
             AND lease_expires_at > CURRENT_TIMESTAMP(6)""".update

  def startRetentionRun(
      runId: String,
      policyName: String,
      targetTable: String,
      cutoff: Instant,
      lease: Lease
  ): Update0 =
    sql"""INSERT INTO retention_runs (
             run_id, policy_name, target_table, cutoff_at, status,
             lease_owner_id, lease_fence, started_at
           ) VALUES (
             $runId, $policyName, $targetTable, ${Timestamp.from(cutoff)}, 'running',
             ${lease.ownerId}, ${lease.fence}, CURRENT_TIMESTAMP(6)
           )""".update

  def finishRetentionRun(
      runId: String,
      status: String,
      rowsSelected: Long,
      rowsArchived: Long,
      rowsDeleted: Long,
      error: Option[String]
  ): Update0 =
    sql"""UPDATE retention_runs
           SET status = $status,
               rows_selected = $rowsSelected,
               rows_archived = $rowsArchived,
               rows_deleted = $rowsDeleted,
               error_text = $error,
               finished_at = CURRENT_TIMESTAMP(6)
           WHERE run_id = $runId AND status = 'running'""".update

  def retentionCandidates(retentionDays: Int, limit: Int): Query0[(String, String, String, Timestamp)] =
    sql"""SELECT e.dedupe_key, e.stream_name, e.payload_sha256, e.observed_at
           FROM sync_events e
           WHERE e.stream_name = 'wireless.audit'
             AND e.status = 'completed'
             AND e.payload_archived = 1
             AND e.payload IS NULL
             AND e.payload_sha256 IS NOT NULL
             AND e.observed_at < TIMESTAMPADD(DAY, -${retentionDays.max(1)}, CURRENT_TIMESTAMP(6))
             AND EXISTS (
               SELECT 1 FROM sync_event_payload_archives archive
               WHERE archive.dedupe_key = e.dedupe_key
                 AND archive.stream_name = e.stream_name
                 AND archive.payload_sha256 = e.payload_sha256
             )
             AND NOT EXISTS (
               SELECT 1 FROM sync_jobs job
               WHERE job.dedupe_key = e.dedupe_key
                 AND job.stream_name = e.stream_name
                 AND job.status NOT IN ('completed', 'failed', 'cancelled')
             )
             AND NOT EXISTS (
               SELECT 1 FROM sync_batches batch
               WHERE batch.dedupe_key = e.dedupe_key
                 AND batch.stream_name = e.stream_name
                 AND batch.status NOT IN ('completed', 'failed', 'cancelled')
             )
             AND NOT EXISTS (
               SELECT 1 FROM sync_batches batch
               JOIN outbox_events outbox ON outbox.source_id = batch.batch_id
               WHERE batch.dedupe_key = e.dedupe_key
                 AND batch.stream_name = e.stream_name
                 AND outbox.status NOT IN ('published', 'failed', 'cancelled')
             )
           ORDER BY e.observed_at, e.dedupe_key
           LIMIT ${limit.max(1)}""".query[(String, String, String, Timestamp)]

  def deleteRetainedEvent(
      candidate: (String, String, String, Timestamp),
      tombstoneDays: Int,
      resourceType: String,
      resourceId: String,
      lease: Lease
  ): ConnectionIO[Boolean] =
    val (dedupeKey, streamName, payloadSha256, observedAt) = candidate
    val leaseGuard =
      sql"""SELECT 1 FROM work_leases
             WHERE resource_type = $resourceType
               AND resource_id = $resourceId
               AND owner_id = ${lease.ownerId}
               AND lease_token = ${lease.token}
               AND fence = ${lease.fence}
               AND lease_expires_at > CURRENT_TIMESTAMP(6)""".query[Int].option

    for
      guarded <- leaseGuard
      _ <- if guarded.contains(1) then FC.unit
           else FC.raiseError(IllegalStateException(s"maintenance lease ${lease.scope} was lost"))
      _ <- sql"""INSERT INTO sync_event_tombstones (
                   dedupe_key, stream_name, payload_sha256, observed_at, expires_at
                 ) VALUES (
                   $dedupeKey, $streamName, $payloadSha256, $observedAt,
                   TIMESTAMPADD(DAY, ${tombstoneDays.max(1)}, CURRENT_TIMESTAMP(6))
                 ) ON DUPLICATE KEY UPDATE
                   expires_at = GREATEST(expires_at, VALUES(expires_at)),
                   updated_at = CURRENT_TIMESTAMP(6)""".update.run
      _ <- sql"""DELETE FROM wireless_frame_security WHERE dedupe_key = $dedupeKey""".update.run
      _ <- sql"""DELETE FROM wireless_frame_identity WHERE dedupe_key = $dedupeKey""".update.run
      _ <- sql"""DELETE FROM wireless_frame_app_signals WHERE dedupe_key = $dedupeKey""".update.run
      _ <- sql"""DELETE FROM wireless_frame_network WHERE dedupe_key = $dedupeKey""".update.run
      _ <- sql"""DELETE FROM wireless_frame_qos WHERE dedupe_key = $dedupeKey""".update.run
      _ <- sql"""DELETE FROM wireless_frame_radio WHERE dedupe_key = $dedupeKey""".update.run
      _ <- sql"""DELETE FROM wireless_frames WHERE dedupe_key = $dedupeKey""".update.run
      _ <- sql"""DELETE attempt FROM outbox_publish_attempts attempt
                   JOIN outbox_events outbox ON outbox.outbox_id = attempt.outbox_id
                   JOIN sync_batches batch ON batch.batch_id = outbox.source_id
                   WHERE batch.dedupe_key = $dedupeKey AND batch.stream_name = $streamName
                     AND outbox.status IN ('published', 'failed', 'cancelled')""".update.run
      _ <- sql"""DELETE outbox FROM outbox_events outbox
                   JOIN sync_batches batch ON batch.batch_id = outbox.source_id
                   WHERE batch.dedupe_key = $dedupeKey AND batch.stream_name = $streamName
                     AND outbox.status IN ('published', 'failed', 'cancelled')""".update.run
      _ <- sql"""DELETE error_row FROM sync_errors error_row
                   LEFT JOIN sync_jobs job ON job.job_id = error_row.job_id
                   LEFT JOIN sync_batches batch ON batch.batch_id = error_row.batch_id
                   WHERE (job.dedupe_key = $dedupeKey AND job.stream_name = $streamName)
                      OR (batch.dedupe_key = $dedupeKey AND batch.stream_name = $streamName)""".update.run
      _ <- sql"""DELETE FROM sync_batches
                   WHERE dedupe_key = $dedupeKey AND stream_name = $streamName
                     AND status IN ('completed', 'failed', 'cancelled')""".update.run
      _ <- sql"""DELETE FROM sync_jobs
                   WHERE dedupe_key = $dedupeKey AND stream_name = $streamName
                     AND status IN ('completed', 'failed', 'cancelled')""".update.run
      _ <- sql"""DELETE FROM sync_backlog
                   WHERE dedupe_key = $dedupeKey AND stream_name = $streamName
                     AND status IN ('synced', 'failed')""".update.run
      deleted <- sql"""DELETE FROM sync_events
                         WHERE dedupe_key = $dedupeKey
                           AND stream_name = $streamName
                           AND status = 'completed'
                           AND payload_archived = 1
                           AND payload IS NULL
                           AND payload_sha256 = $payloadSha256""".update.run
      _ <- if deleted == 1 then FC.unit
           else FC.raiseError(IllegalStateException(s"retention candidate $streamName/$dedupeKey changed"))
    yield true

  def pruneTombstones(limit: Int): Update0 =
    sql"""DELETE FROM sync_event_tombstones
           WHERE expires_at < CURRENT_TIMESTAMP(6)
           ORDER BY expires_at, stream_name, dedupe_key
           LIMIT ${limit.max(1)}""".update

  def searchRetentionCandidates(retentionDays: Int, limit: Int): Query0[String] =
    sql"""SELECT document.document_id
           FROM atheros_search.search_documents document
           WHERE document.status IN ('superseded', 'deleted', 'failed')
             AND document.updated_at < TIMESTAMPADD(DAY, -${retentionDays.max(1)}, CURRENT_TIMESTAMP(6))
             AND NOT EXISTS (
               SELECT 1 FROM atheros_search.embedding_jobs job
               WHERE job.document_id = document.document_id
                 AND job.status NOT IN ('completed', 'failed', 'cancelled')
             )
           ORDER BY document.updated_at, document.document_id
           LIMIT ${limit.max(1)}""".query[String]

  def deleteRetainedSearchDocument(
      documentId: String,
      resourceType: String,
      resourceId: String,
      lease: Lease
  ): ConnectionIO[Boolean] =
    val leaseGuard =
      sql"""SELECT 1 FROM work_leases
             WHERE resource_type = $resourceType
               AND resource_id = $resourceId
               AND owner_id = ${lease.ownerId}
               AND lease_token = ${lease.token}
               AND fence = ${lease.fence}
               AND lease_expires_at > CURRENT_TIMESTAMP(6)""".query[Int].option

    for
      guarded <- leaseGuard
      _ <- if guarded.contains(1) then FC.unit
           else FC.raiseError(IllegalStateException(s"maintenance lease ${lease.scope} was lost"))
      _ <- sql"""DELETE FROM atheros_search.search_vectors_event
                   WHERE document_id = $documentId""".update.run
      _ <- sql"""DELETE FROM atheros_search.search_vectors_device
                   WHERE document_id = $documentId""".update.run
      _ <- sql"""DELETE FROM atheros_search.search_vectors_behaviour
                   WHERE document_id = $documentId""".update.run
      _ <- sql"""DELETE FROM atheros_search.search_vectors_sequence
                   WHERE document_id = $documentId""".update.run
      _ <- sql"""DELETE FROM atheros_search.embedding_jobs
                   WHERE document_id = $documentId
                     AND status IN ('completed', 'failed', 'cancelled')""".update.run
      _ <- sql"""DELETE FROM atheros_search.search_document_tokens
                   WHERE document_id = $documentId""".update.run
      _ <- sql"""DELETE FROM atheros_search.search_document_tags
                   WHERE document_id = $documentId""".update.run
      deleted <- sql"""DELETE FROM atheros_search.search_documents
                         WHERE document_id = $documentId
                           AND status IN ('superseded', 'deleted', 'failed')""".update.run
      _ <- if deleted == 1 then FC.unit
           else FC.raiseError(IllegalStateException(s"search retention candidate $documentId changed"))
    yield true

  def cleanupStaleWorkers(limit: Int): ConnectionIO[Int] =
    val batchLimit = limit.max(1)
    for
      jobs <- sql"""UPDATE sync_jobs
                     SET status = CASE WHEN attempt_count >= max_attempts THEN 'failed' ELSE 'pending' END,
                         owner_id = NULL,
                         lease_token = NULL,
                         lease_expires_at = NULL,
                         next_attempt_at = CURRENT_TIMESTAMP(6),
                         last_error = CASE
                           WHEN attempt_count >= max_attempts THEN 'worker lease expired; retries exhausted'
                           ELSE 'worker lease expired; returned to pending'
                         END,
                         updated_at = CURRENT_TIMESTAMP(6)
                     WHERE status IN ('leased', 'running')
                       AND lease_expires_at <= CURRENT_TIMESTAMP(6)
                     ORDER BY lease_expires_at, job_id
                     LIMIT $batchLimit""".update.run
      batches <- sql"""UPDATE sync_batches
                        SET status = CASE WHEN attempt_count >= max_attempts THEN 'failed' ELSE 'pending' END,
                            owner_id = NULL,
                            lease_token = NULL,
                            lease_expires_at = NULL,
                            next_attempt_at = CURRENT_TIMESTAMP(6),
                            last_error = CASE
                              WHEN attempt_count >= max_attempts THEN 'worker lease expired; retries exhausted'
                              ELSE 'worker lease expired; returned to pending'
                            END,
                            updated_at = CURRENT_TIMESTAMP(6)
                        WHERE status IN ('leased', 'processing')
                          AND lease_expires_at <= CURRENT_TIMESTAMP(6)
                        ORDER BY lease_expires_at, batch_id
                        LIMIT $batchLimit""".update.run
      workLeases <- sql"""UPDATE work_leases
                           SET owner_id = NULL,
                               lease_token = NULL,
                               lease_expires_at = NULL,
                               next_attempt_at = CURRENT_TIMESTAMP(6),
                               last_error = 'worker lease expired; released by cleanup',
                               updated_at = CURRENT_TIMESTAMP(6)
                           WHERE lease_expires_at <= CURRENT_TIMESTAMP(6)
                           ORDER BY lease_expires_at, resource_type, resource_id
                           LIMIT $batchLimit""".update.run
    yield jobs + batches + workLeases

  def reconcileMissingWirelessChildren(limit: Int): ConnectionIO[Int] =
    val batchLimit = limit.max(1)
    sql"""INSERT INTO reconciliation_findings (
             finding_id, processor_name, entity_type, entity_key,
             projection_version, finding_type, status, repair_action, details
           )
           SELECT UUID(), 'wireless-frame-normalizer', 'wireless_frame', frame.dedupe_key,
                  1, 'missing_child_projection', 'open', 'rebuild_wireless_frame',
                  JSON_OBJECT(
                    'radio_missing', radio.dedupe_key IS NULL,
                    'qos_missing', qos.dedupe_key IS NULL,
                    'network_missing', network_row.dedupe_key IS NULL,
                    'app_signals_missing', app_row.dedupe_key IS NULL,
                    'identity_missing', identity_row.dedupe_key IS NULL,
                    'security_missing', security_row.dedupe_key IS NULL
                  )
           FROM wireless_frames frame
           LEFT JOIN wireless_frame_radio radio ON radio.dedupe_key = frame.dedupe_key
           LEFT JOIN wireless_frame_qos qos ON qos.dedupe_key = frame.dedupe_key
           LEFT JOIN wireless_frame_network network_row ON network_row.dedupe_key = frame.dedupe_key
           LEFT JOIN wireless_frame_app_signals app_row ON app_row.dedupe_key = frame.dedupe_key
           LEFT JOIN wireless_frame_identity identity_row ON identity_row.dedupe_key = frame.dedupe_key
           LEFT JOIN wireless_frame_security security_row ON security_row.dedupe_key = frame.dedupe_key
           WHERE frame.observed_at >= TIMESTAMPADD(HOUR, -24, CURRENT_TIMESTAMP(6))
             AND (radio.dedupe_key IS NULL
                  OR qos.dedupe_key IS NULL
                  OR network_row.dedupe_key IS NULL
                  OR app_row.dedupe_key IS NULL
                  OR identity_row.dedupe_key IS NULL
                  OR security_row.dedupe_key IS NULL)
           ORDER BY frame.observed_at DESC, frame.dedupe_key
           LIMIT $batchLimit
           ON DUPLICATE KEY UPDATE
             details = VALUES(details),
             last_seen_at = CURRENT_TIMESTAMP(6),
             status = IF(status = 'resolved', 'open', status)""".update.run

  val ResolveWirelessFindings: Update0 =
    sql"""UPDATE reconciliation_findings finding
           SET finding.status = 'resolved',
               finding.resolved_at = CURRENT_TIMESTAMP(6),
               finding.last_seen_at = CURRENT_TIMESTAMP(6)
           WHERE finding.processor_name = 'wireless-frame-normalizer'
             AND finding.finding_type = 'missing_child_projection'
             AND finding.status IN ('open', 'repairing')
             AND EXISTS (SELECT 1 FROM wireless_frame_radio child WHERE child.dedupe_key = finding.entity_key)
             AND EXISTS (SELECT 1 FROM wireless_frame_qos child WHERE child.dedupe_key = finding.entity_key)
             AND EXISTS (SELECT 1 FROM wireless_frame_network child WHERE child.dedupe_key = finding.entity_key)
             AND EXISTS (SELECT 1 FROM wireless_frame_app_signals child WHERE child.dedupe_key = finding.entity_key)
             AND EXISTS (SELECT 1 FROM wireless_frame_identity child WHERE child.dedupe_key = finding.entity_key)
             AND EXISTS (SELECT 1 FROM wireless_frame_security child WHERE child.dedupe_key = finding.entity_key)""".update
