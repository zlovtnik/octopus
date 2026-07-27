package com.sslproxy.coordinator.tidb

import cats.effect.IO
import cats.effect.implicits.*
import cats.effect.std.Semaphore
import cats.syntax.all.*
import com.sslproxy.coordinator.cutover.CutoffKey
import com.sslproxy.coordinator.domain.{BrokerRecordMetadata, DatabaseError, IngestionDecision, IngestionDisposition, ResolvedScanRequestRecord, SyncLoad}
import doobie.*
import doobie.implicits.*
import io.circe.{Json, parser as circeParser}
import io.circe.syntax.*
import com.sslproxy.coordinator.observability.StructuredLogger
import com.sslproxy.coordinator.util.Sha256Utils

import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.concurrent.duration.*

class TidbRepository(xa: Transactor[IO]):
  import TidbRepository.log

  def checkConnectivity(): IO[Either[DatabaseError, Unit]] =
    runDb("tidb.check_connectivity") {
      sql"SELECT 1".query[Int].unique.map(_ => ())
    }

  def pendingLedgerCount(): IO[Either[DatabaseError, Long]] =
    runDb("tidb.pending_ledger_count") {
      sql"""SELECT COUNT(*) FROM sync_events
            WHERE status IN ('pending', 'processing')"""
        .query[Long]
        .unique
    }

  def loadConsumerOffsets(
      groupId: String,
      topic: String
  ): IO[Either[DatabaseError, Set[CutoffKey]]] =
    runDb("tidb.load_consumer_offsets") {
      sql"""SELECT partition_id FROM consumer_offsets
            WHERE group_id = $groupId AND topic = $topic"""
        .query[Int].to[List].map { partitions =>
          partitions.map(p => CutoffKey(groupId, topic, p)).toSet
        }
    }

  def processIngestLedger(
      streamNames: List[String],
      loadStreamNames: List[String],
      scanMaxAttempts: Int,
      scanRetryBackoffSeconds: Int,
      ingestBatchSize: Int
  ): IO[Either[DatabaseError, Long]] =
    runDb("tidb.process_ingest_ledger") {
      val limit = ingestBatchSize max 1
      val backoffSecs = scanRetryBackoffSeconds max 1

      if streamNames.isEmpty then 0L.pure[ConnectionIO]
      else
        val streamClause = streamNames.map(value => fr0"$value").intercalate(fr",")
        val loadClause = loadStreamNames.map(value => fr0"$value").intercalate(fr",")
        val maxAttempts = scanMaxAttempts.max(1)
        val candidates =
          fr"""FROM sync_events e
               WHERE e.stream_name IN (""" ++ streamClause ++ fr""" )
                 AND e.status IN ('pending', 'failed')
                 AND e.attempt_count < $maxAttempts
                 AND (e.status = 'pending' OR e.updated_at <= TIMESTAMPADD(SECOND, -$backoffSecs, CURRENT_TIMESTAMP(6)))
               ORDER BY e.observed_at, e.stream_name, e.dedupe_key
               LIMIT $limit"""

        val insertJobs =
          fr"""INSERT INTO sync_jobs (
                 job_id, stream_name, dedupe_key, status, attempt_count, created_at
               )
               SELECT UUID(), e.stream_name, e.dedupe_key, 'pending', 0, CURRENT_TIMESTAMP(6) """ ++
            candidates ++
            fr""" ON DUPLICATE KEY UPDATE job_id = sync_jobs.job_id"""

        val insertBatches =
          fr"""INSERT INTO sync_batches (
                 batch_id, job_id, batch_no, payload_ref, status, row_count,
                 attempt_count, dedupe_key, stream_name, cursor_start, cursor_end,
                 created_at, updated_at
               )
               SELECT UUID(), j.job_id, 0, e.payload_ref, 'pending', 1,
                      0, e.dedupe_key, e.stream_name,
                      COALESCE(c.cursor_value, '0'), e.dedupe_key,
                      CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6)
               FROM sync_events e
               JOIN sync_jobs j
                 ON j.stream_name = e.stream_name AND j.dedupe_key = e.dedupe_key
                LEFT JOIN sync_cursors c ON c.stream_name = e.stream_name
                WHERE e.status IN ('pending', 'failed')
                  AND e.stream_name IN (""" ++ streamClause ++ fr""" )
                  AND e.attempt_count < $maxAttempts
                  AND (e.status = 'pending' OR e.updated_at <= TIMESTAMPADD(SECOND, -$backoffSecs, CURRENT_TIMESTAMP(6)))
                ORDER BY e.observed_at, e.stream_name, e.dedupe_key
                LIMIT $limit
                ON DUPLICATE KEY UPDATE batch_id = sync_batches.batch_id"""

        val insertOutbox =
          if loadStreamNames.isEmpty then 0.pure[ConnectionIO]
          else
            (fr"""INSERT INTO outbox_events (
                    outbox_id, source_type, source_id, event_type,
                    destination_topic, message_key, payload, status,
                    attempt_count, max_attempts, next_attempt_at, created_at, updated_at
                  )
                  SELECT UUID(), 'sync_batch', b.batch_id, 'sync.load.requested',
                         'sync.oracle.load', CONCAT(b.batch_id, ':', b.attempt_count + 1),
                         JSON_OBJECT(
                           'job_id', b.job_id,
                           'batch_id', b.batch_id,
                           'batch_no', b.batch_no,
                           'stream_name', b.stream_name,
                           'payload_ref', b.payload_ref,
                           'cursor_start', b.cursor_start,
                           'cursor_end', b.cursor_end,
                           'attempt', b.attempt_count + 1
                         ),
                         'pending', 0, $maxAttempts, CURRENT_TIMESTAMP(6),
                         CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6)
                  FROM sync_batches b
                  WHERE b.status = 'pending'
                    AND b.stream_name IN (""" ++ loadClause ++ fr""" )
                  ORDER BY b.created_at, b.batch_id
                  LIMIT $limit
                  ON DUPLICATE KEY UPDATE payload = VALUES(payload), updated_at = CURRENT_TIMESTAMP(6)""").update.run

        for
          _ <- insertJobs.update.run
          _ <- insertBatches.update.run
          _ <- insertOutbox
          processed <- (fr"""UPDATE sync_events e
                              SET e.status = 'batched',
                                  e.attempt_count = e.attempt_count + 1,
                                  e.last_error = NULL,
                                  e.updated_at = CURRENT_TIMESTAMP(6)
                              WHERE e.status IN ('pending', 'failed')
                                AND e.stream_name IN (""" ++ streamClause ++ fr""" )
                                AND e.attempt_count < $maxAttempts
                                AND (e.status = 'pending' OR e.updated_at <= TIMESTAMPADD(SECOND, -$backoffSecs, CURRENT_TIMESTAMP(6)))
                                AND EXISTS (
                                  SELECT 1 FROM sync_batches b
                                  WHERE b.stream_name = e.stream_name
                                    AND b.dedupe_key = e.dedupe_key
                                )""").update.run
        yield processed.toLong
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

  def findSyncEventsNeedingHydration(
      after: Option[SyncEventHydrationCandidate],
      limit: Int
  ): IO[Either[DatabaseError, List[SyncEventHydrationCandidate]]] =
    runDb("tidb.find_sync_events_needing_hydration") {
      val cursorClause = after.fold(Fragment.empty) { cursor =>
        fr"""AND (
              e.observed_at > ${cursor.observedAt}
              OR (e.observed_at = ${cursor.observedAt} AND e.stream_name > ${cursor.streamName})
              OR (e.observed_at = ${cursor.observedAt}
                  AND e.stream_name = ${cursor.streamName}
                  AND e.dedupe_key > ${cursor.dedupeKey})
            )"""
      }
      (fr"""SELECT e.dedupe_key, e.stream_name, e.observed_at, e.payload_ref,
                   CAST(e.payload AS CHAR)
            FROM sync_events e
            WHERE e.payload_archived = 0
              AND NOT EXISTS (
                SELECT 1
                FROM sync_event_tombstones tombstone
                WHERE tombstone.dedupe_key = e.dedupe_key
                  AND tombstone.stream_name = e.stream_name
                  AND tombstone.expires_at > CURRENT_TIMESTAMP(6)
              )
              AND (
                e.payload IS NULL
                OR (
                  e.stream_name = 'wireless.audit'
                  AND (
                    e.event_type IS NULL
                    OR e.schema_version IS NULL
                    OR e.sensor_id IS NULL
                    OR e.wireless_search_text IS NULL
                  )
                )
              )""" ++ cursorClause ++
        fr"""ORDER BY e.observed_at, e.stream_name, e.dedupe_key
            LIMIT ${limit.max(1)}""")
        .query[(String, String, java.sql.Timestamp, String, Option[String])]
        .to[List]
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
    val payload = Option(record.payloadJson)
    val jobId = stableUuid(s"job:${record.streamName}:${record.dedupeKey}")
    val batchId = stableUuid(s"batch:${record.streamName}:${record.dedupeKey}")
    val loadMessageKey = s"$batchId:1"
    val outboxId = stableUuid(s"outbox:sync.oracle.load:$loadMessageKey")

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
      sql"""SELECT payload_sha256, artifact_sha256, dedupe_key
            FROM ingestion_evidence
            WHERE topic = ${meta.topic}
              AND partition_id = ${meta.partition}
              AND record_offset = ${meta.offset}
              AND group_id = ${meta.consumerGroup}"""
        .query[(String, String, String)]
        .option

    def verifyExisting(meta: BrokerRecordMetadata, existing: (String, String, String)): ConnectionIO[Unit] =
      val (payloadSha, cutoverSha, dedupeKey) = existing
      if payloadSha != record.sourceRecordSha256 || cutoverSha != meta.artifactSha256 || dedupeKey != record.dedupeKey then
        FC.raiseError(IllegalStateException(
          s"broker coordinate ${meta.topic}/${meta.partition}/${meta.offset}/${meta.consumerGroup} changed after ingestion"
        ))
      else ().pure[ConnectionIO]

    def persistEvidence(meta: BrokerRecordMetadata, disposition: IngestionDisposition): ConnectionIO[Unit] =
      sql"""INSERT INTO ingestion_evidence (
              topic, partition_id, record_offset, group_id, group_version,
              artifact_sha256, message_key, payload_sha256, disposition,
              dedupe_key, first_seen_at, updated_at
            ) VALUES (
              ${meta.topic}, ${meta.partition}, ${meta.offset}, ${meta.consumerGroup}, ${meta.groupVersion},
              ${meta.artifactSha256}, ${meta.messageKey}, ${record.sourceRecordSha256}, ${disposition.databaseValue},
              ${record.dedupeKey}, CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6)
            )""".update.run.void *>
        advanceConsumerOffset(meta)

    def createState: ConnectionIO[IngestionDecision] =
      for
        tombstoned <- sql"""SELECT EXISTS(
                              SELECT 1 FROM sync_event_tombstones
                              WHERE stream_name = ${record.streamName}
                                AND dedupe_key = ${record.dedupeKey}
                                AND expires_at > CURRENT_TIMESTAMP(6)
                            )""".query[Int].unique.map(_ == 1)
        decision <-
          if tombstoned then
            IngestionDecision(IngestionDisposition.Deduplicated, record.dedupeKey, jobId, batchId).pure[ConnectionIO]
          else
            for
              inserted <- sql"""INSERT IGNORE INTO sync_events (
                                   dedupe_key, stream_name, observed_at, payload_ref, payload,
                                   payload_sha256, status, attempt_count, last_error,
                                   producer, event_kind, created_at, updated_at
                                 ) VALUES (
                                   ${record.dedupeKey}, ${record.streamName}, ${observedAt.orNull}, $payloadRef, $payload,
                                   ${record.eventPayloadSha256}, 'batched', 1, NULL,
                                   'ssl-proxy', $eventKind, CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6)
                                 )""".update.run
              _ <- hydrateSyncEvent(record, eventKind)
              _ <- sql"""INSERT INTO sync_jobs (
                            job_id, stream_name, dedupe_key, status, attempt_count, created_at
                          ) VALUES (
                            $jobId, ${record.streamName}, ${record.dedupeKey}, 'pending', 0, CURRENT_TIMESTAMP(6)
                          ) ON DUPLICATE KEY UPDATE job_id = sync_jobs.job_id""".update.run
              cursor <- sql"""SELECT cursor_value FROM sync_cursors
                               WHERE stream_name = ${record.streamName}""".query[String].option.map(_.getOrElse("0"))
              _ <- sql"""INSERT INTO sync_batches (
                            batch_id, job_id, batch_no, payload_ref, status, row_count,
                            attempt_count, dedupe_key, stream_name, cursor_start, cursor_end,
                            created_at, updated_at
                          ) VALUES (
                            $batchId, $jobId, 0, $payloadRef, 'pending', 1,
                            0, ${record.dedupeKey}, ${record.streamName}, $cursor, ${record.dedupeKey},
                            CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6)
                          ) ON DUPLICATE KEY UPDATE batch_id = sync_batches.batch_id""".update.run
              load = SyncLoad(
                jobId = jobId,
                batchId = batchId,
                batchNo = Some(0),
                streamName = record.streamName,
                payloadRef = payloadRef,
                cursorStart = cursor,
                cursorEnd = record.dedupeKey,
                attempt = 1
              )
              _ <- sql"""INSERT INTO outbox_events (
                            outbox_id, source_type, source_id, event_type,
                            destination_topic, message_key, payload, status,
                            attempt_count, max_attempts, next_attempt_at, created_at, updated_at
                          ) VALUES (
                            $outboxId, 'sync_batch', $batchId, 'sync.load.requested',
                            'sync.oracle.load', $loadMessageKey, ${load.asJson.noSpaces}, 'pending',
                            0, 5, CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6)
                          ) ON DUPLICATE KEY UPDATE outbox_id = outbox_events.outbox_id""".update.run
              disposition = if inserted == 1 then IngestionDisposition.Processed else IngestionDisposition.Deduplicated
            yield IngestionDecision(disposition, record.dedupeKey, jobId, batchId)
      yield decision

    validate *> (metadata match
      case None => createState
      case Some(meta) =>
        existingEvidence(meta).flatMap {
          case Some(existing) =>
            verifyExisting(meta, existing) *>
              hydrateSyncEvent(record, eventKind).as(
                IngestionDecision(IngestionDisposition.Deduplicated, record.dedupeKey, jobId, batchId)
              )
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

            updated <- sql"""UPDATE sync_events
                             SET payload = $payloadJson,
                                 payload_sha256 = $eventPayloadSha256,
                                 event_kind = COALESCE($eventKind, event_kind),
                                 updated_at = CURRENT_TIMESTAMP(6)
                             WHERE dedupe_key = $key
                               AND stream_name = $stream
                               AND payload_archived = 0
                               AND (payload IS NULL OR payload_sha256 = $eventPayloadSha256)
                               AND NOT EXISTS (
                                 SELECT 1
                                 FROM sync_event_tombstones tombstone
                                 WHERE tombstone.dedupe_key = sync_events.dedupe_key
                                   AND tombstone.stream_name = sync_events.stream_name
                                   AND tombstone.expires_at > CURRENT_TIMESTAMP(6)
                               )""".update.run
            _ <- if updated > 0 && stream == "wireless.audit" then
              hydrateWirelessProjection(key)
            else 0.pure[ConnectionIO]
          yield updated > 0
      }

  private def hydrateWirelessProjection(dedupeKey: String): ConnectionIO[Int] =
    val project =
      sql"""UPDATE sync_events
            SET
              sensor_id = NULLIF(payload->>'$$.sensor_id', ''),
              location_id = NULLIF(payload->>'$$.location_id', ''),
              username = NULLIF(payload->>'$$.username', ''),
              event_type = COALESCE(
                NULLIF(payload->>'$$.event_type', ''),
                NULLIF(payload->>'$$.type', '')
              ),
              schema_version = CAST(NULLIF(payload->>'$$.schema_version', '') AS SIGNED),
              frame_type = COALESCE(
                NULLIF(payload->>'$$.frame_type', ''),
                NULLIF(payload->>'$$.mac.frame_type', '')
              ),
              frame_subtype = COALESCE(
                NULLIF(payload->>'$$.frame_subtype', ''),
                NULLIF(payload->>'$$.mac.frame_subtype', '')
              ),
              source_mac = LOWER(COALESCE(
                NULLIF(payload->>'$$.source_mac', ''),
                NULLIF(payload->>'$$.mac.source_mac', '')
              )),
              transmitter_mac = LOWER(COALESCE(
                NULLIF(payload->>'$$.transmitter_mac', ''),
                NULLIF(payload->>'$$.mac.transmitter_mac', '')
              )),
              receiver_mac = LOWER(COALESCE(
                NULLIF(payload->>'$$.receiver_mac', ''),
                NULLIF(payload->>'$$.mac.receiver_mac', '')
              )),
              bssid = LOWER(COALESCE(
                NULLIF(payload->>'$$.bssid', ''),
                NULLIF(payload->>'$$.mac.bssid', '')
              )),
              destination_bssid = LOWER(COALESCE(
                NULLIF(payload->>'$$.destination_bssid', ''),
                NULLIF(payload->>'$$.destination_mac', ''),
                NULLIF(payload->>'$$.mac.destination_mac', ''),
                NULLIF(payload->>'$$.mac.bssid', '')
              )),
              ssid = NULLIF(payload->>'$$.ssid', ''),
              signal_dbm = CAST(NULLIF(COALESCE(
                NULLIF(payload->>'$$.signal_dbm', ''),
                NULLIF(payload->>'$$.rf.signal_dbm', '')
              ), '') AS SIGNED),
              noise_dbm = CAST(NULLIF(COALESCE(
                NULLIF(payload->>'$$.noise_dbm', ''),
                NULLIF(payload->>'$$.rf.noise_dbm', '')
              ), '') AS SIGNED),
              frequency_mhz = CAST(NULLIF(COALESCE(
                NULLIF(payload->>'$$.frequency_mhz', ''),
                NULLIF(payload->>'$$.rf.frequency_mhz', '')
              ), '') AS SIGNED),
              channel_flags = CAST(NULLIF(COALESCE(
                NULLIF(payload->>'$$.channel_flags', ''),
                NULLIF(payload->>'$$.rf.channel_flags.raw', '')
              ), '') AS SIGNED),
              data_rate_kbps = CAST(NULLIF(COALESCE(
                NULLIF(payload->>'$$.data_rate_kbps', ''),
                NULLIF(payload->>'$$.rf.data_rate_kbps', '')
              ), '') AS SIGNED),
              antenna_id = CAST(NULLIF(COALESCE(
                NULLIF(payload->>'$$.antenna_id', ''),
                NULLIF(payload->>'$$.rf.antenna_id', '')
              ), '') AS SIGNED),
              tsft = CAST(NULLIF(COALESCE(
                NULLIF(payload->>'$$.tsft', ''),
                NULLIF(payload->>'$$.rf.tsft', '')
              ), '') AS SIGNED),
              fragment_number = CAST(NULLIF(COALESCE(
                NULLIF(payload->>'$$.fragment_number', ''),
                NULLIF(payload->>'$$.mac.fragment_number', '')
              ), '') AS SIGNED),
              channel_number = CAST(NULLIF(COALESCE(
                NULLIF(payload->>'$$.channel_number', ''),
                NULLIF(payload->>'$$.channel', ''),
                NULLIF(payload->>'$$.rf.channel_number', '')
              ), '') AS SIGNED),
              signal_status = COALESCE(
                NULLIF(payload->>'$$.signal_status', ''),
                NULLIF(payload->>'$$.rf.signal_status', '')
              ),
              adjacent_mac_hint = LOWER(COALESCE(
                NULLIF(payload->>'$$.adjacent_mac_hint', ''),
                NULLIF(payload->>'$$.mac.adjacent_mac_hint', '')
              )),
              qos_tid = CAST(NULLIF(COALESCE(
                NULLIF(payload->>'$$.qos_tid', ''),
                NULLIF(payload->>'$$.qos.tid', '')
              ), '') AS SIGNED),
              qos_eosp = CASE LOWER(COALESCE(
                NULLIF(payload->>'$$.qos_eosp', ''),
                NULLIF(payload->>'$$.qos.eosp', '')
              )) WHEN 'true' THEN 1 WHEN 'false' THEN 0 WHEN '1' THEN 1 WHEN '0' THEN 0 ELSE NULL END,
              qos_ack_policy = CAST(NULLIF(COALESCE(
                NULLIF(payload->>'$$.qos_ack_policy', ''),
                NULLIF(payload->>'$$.qos.ack_policy', '')
              ), '') AS SIGNED),
              qos_ack_policy_label = COALESCE(
                NULLIF(payload->>'$$.qos_ack_policy_label', ''),
                NULLIF(payload->>'$$.qos.ack_policy_label', '')
              ),
              qos_amsdu = CASE LOWER(COALESCE(
                NULLIF(payload->>'$$.qos_amsdu', ''),
                NULLIF(payload->>'$$.qos.amsdu', '')
              )) WHEN 'true' THEN 1 WHEN 'false' THEN 0 WHEN '1' THEN 1 WHEN '0' THEN 0 ELSE NULL END,
              llc_oui = COALESCE(
                NULLIF(payload->>'$$.llc_oui', ''),
                NULLIF(payload->>'$$.llc_snap.oui', '')
              ),
              ethertype = CAST(NULLIF(COALESCE(
                NULLIF(payload->>'$$.ethertype', ''),
                NULLIF(payload->>'$$.llc_snap.ethertype', '')
              ), '') AS SIGNED),
              ethertype_name = COALESCE(
                NULLIF(payload->>'$$.ethertype_name', ''),
                NULLIF(payload->>'$$.llc_snap.ethertype_name', '')
              ),
              src_ip = COALESCE(
                NULLIF(payload->>'$$.src_ip', ''),
                NULLIF(payload->>'$$.network.src_ip', '')
              ),
              dst_ip = COALESCE(
                NULLIF(payload->>'$$.dst_ip', ''),
                NULLIF(payload->>'$$.network.dst_ip', '')
              ),
              ip_ttl = CAST(NULLIF(COALESCE(
                NULLIF(payload->>'$$.ip_ttl', ''),
                NULLIF(payload->>'$$.network.ttl', '')
              ), '') AS SIGNED),
              ip_protocol = CAST(NULLIF(COALESCE(
                NULLIF(payload->>'$$.ip_protocol', ''),
                NULLIF(payload->>'$$.network.protocol', '')
              ), '') AS SIGNED),
              ip_protocol_name = COALESCE(
                NULLIF(payload->>'$$.ip_protocol_name', ''),
                NULLIF(payload->>'$$.network.protocol_name', '')
              ),
              src_port = CAST(NULLIF(COALESCE(
                NULLIF(payload->>'$$.src_port', ''),
                NULLIF(payload->>'$$.transport.src_port', '')
              ), '') AS SIGNED),
              dst_port = CAST(NULLIF(COALESCE(
                NULLIF(payload->>'$$.dst_port', ''),
                NULLIF(payload->>'$$.transport.dst_port', '')
              ), '') AS SIGNED),
              transport_protocol = COALESCE(
                NULLIF(payload->>'$$.transport_protocol', ''),
                NULLIF(payload->>'$$.transport.protocol', '')
              ),
              transport_length = CAST(NULLIF(COALESCE(
                NULLIF(payload->>'$$.transport_length', ''),
                NULLIF(payload->>'$$.transport.length', '')
              ), '') AS SIGNED),
              transport_checksum = CAST(NULLIF(COALESCE(
                NULLIF(payload->>'$$.transport_checksum', ''),
                NULLIF(payload->>'$$.transport.checksum', '')
              ), '') AS SIGNED),
              app_protocol = COALESCE(
                NULLIF(payload->>'$$.app_protocol', ''),
                NULLIF(payload->>'$$.application.protocol', '')
              ),
              ssdp_message_type = COALESCE(
                NULLIF(payload->>'$$.ssdp_message_type', ''),
                NULLIF(payload->>'$$.application.ssdp.message_type', '')
              ),
              ssdp_st = COALESCE(
                NULLIF(payload->>'$$.ssdp_st', ''),
                NULLIF(payload->>'$$.application.ssdp.st', '')
              ),
              ssdp_mx = COALESCE(
                NULLIF(payload->>'$$.ssdp_mx', ''),
                NULLIF(payload->>'$$.application.ssdp.mx', '')
              ),
              ssdp_usn = COALESCE(
                NULLIF(payload->>'$$.ssdp_usn', ''),
                NULLIF(payload->>'$$.application.ssdp.usn', '')
              ),
              dhcp_requested_ip = COALESCE(
                NULLIF(payload->>'$$.dhcp_requested_ip', ''),
                NULLIF(payload->>'$$.application.dhcp.requested_ip', '')
              ),
              dhcp_hostname = COALESCE(
                NULLIF(payload->>'$$.dhcp_hostname', ''),
                NULLIF(payload->>'$$.application.dhcp.hostname', '')
              ),
              dhcp_vendor_class = COALESCE(
                NULLIF(payload->>'$$.dhcp_vendor_class', ''),
                NULLIF(payload->>'$$.application.dhcp.vendor_class', '')
              ),
              dns_query_name = COALESCE(
                NULLIF(payload->>'$$.dns_query_name', ''),
                NULLIF(payload->>'$$.application.dns.query_names[0]', '')
              ),
              mdns_name = COALESCE(
                NULLIF(payload->>'$$.mdns_name', ''),
                NULLIF(payload->>'$$.application.mdns.query_names[0]', '')
              ),
              session_key = COALESCE(
                NULLIF(payload->>'$$.session_key', ''),
                NULLIF(payload->>'$$.correlation.session_key', '')
              ),
              retransmit_key = COALESCE(
                NULLIF(payload->>'$$.retransmit_key', ''),
                NULLIF(payload->>'$$.correlation.retransmit_key', '')
              ),
              frame_fingerprint = COALESCE(
                NULLIF(payload->>'$$.frame_fingerprint', ''),
                NULLIF(payload->>'$$.correlation.frame_fingerprint', '')
              ),
              payload_visibility = COALESCE(
                NULLIF(payload->>'$$.payload_visibility', ''),
                NULLIF(payload->>'$$.correlation.payload_visibility', '')
              ),
              tsft_delta_us = CAST(NULLIF(COALESCE(
                NULLIF(payload->>'$$.tsft_delta_us', ''),
                NULLIF(payload->>'$$.correlation.tsft_delta_us', '')
              ), '') AS SIGNED),
              wall_clock_delta_ms = CAST(NULLIF(COALESCE(
                NULLIF(payload->>'$$.wall_clock_delta_ms', ''),
                NULLIF(payload->>'$$.correlation.wall_clock_delta_ms', '')
              ), '') AS SIGNED),
              large_frame = COALESCE(CASE LOWER(COALESCE(
                NULLIF(payload->>'$$.large_frame', ''),
                NULLIF(payload->>'$$.anomalies.large_frame', '')
              )) WHEN 'true' THEN 1 WHEN 'false' THEN 0 WHEN '1' THEN 1 WHEN '0' THEN 0 ELSE NULL END, 0),
              mixed_encryption = CASE LOWER(COALESCE(
                NULLIF(payload->>'$$.mixed_encryption', ''),
                NULLIF(payload->>'$$.anomalies.mixed_encryption', '')
              )) WHEN 'true' THEN 1 WHEN 'false' THEN 0 WHEN '1' THEN 1 WHEN '0' THEN 0 ELSE NULL END,
              dedupe_or_replay_suspect = COALESCE(CASE LOWER(COALESCE(
                NULLIF(payload->>'$$.dedupe_or_replay_suspect', ''),
                NULLIF(payload->>'$$.anomalies.dedupe_or_replay_suspect', '')
              )) WHEN 'true' THEN 1 WHEN 'false' THEN 0 WHEN '1' THEN 1 WHEN '0' THEN 0 ELSE NULL END, 0),
              raw_len = COALESCE(CAST(NULLIF(COALESCE(
                NULLIF(payload->>'$$.raw_len', ''),
                NULLIF(payload->>'$$.rf.raw_len', '')
              ), '') AS SIGNED), 0),
              frame_control_flags = COALESCE(CAST(NULLIF(payload->>'$$.frame_control_flags', '') AS SIGNED), 0),
              more_data = COALESCE(CASE LOWER(COALESCE(
                NULLIF(payload->>'$$.more_data', ''),
                NULLIF(payload->>'$$.mac.more_data', '')
              )) WHEN 'true' THEN 1 WHEN 'false' THEN 0 WHEN '1' THEN 1 WHEN '0' THEN 0 ELSE NULL END, 0),
              retry = COALESCE(CASE LOWER(COALESCE(
                NULLIF(payload->>'$$.retry', ''),
                NULLIF(payload->>'$$.mac.retry', '')
              )) WHEN 'true' THEN 1 WHEN 'false' THEN 0 WHEN '1' THEN 1 WHEN '0' THEN 0 ELSE NULL END, 0),
              power_save = COALESCE(CASE LOWER(COALESCE(
                NULLIF(payload->>'$$.power_save', ''),
                NULLIF(payload->>'$$.mac.power_save', '')
              )) WHEN 'true' THEN 1 WHEN 'false' THEN 0 WHEN '1' THEN 1 WHEN '0' THEN 0 ELSE NULL END, 0),
              protected = COALESCE(CASE LOWER(COALESCE(
                NULLIF(payload->>'$$.protected', ''),
                NULLIF(payload->>'$$.mac.protected', '')
              )) WHEN 'true' THEN 1 WHEN 'false' THEN 0 WHEN '1' THEN 1 WHEN '0' THEN 0 ELSE NULL END, 0),
              security_flags = COALESCE(CAST(NULLIF(payload->>'$$.security_flags', '') AS SIGNED), 0),
              risk_score = CAST(NULLIF(payload->>'$$.risk_score', '') AS DOUBLE),
              identity_source = NULLIF(payload->>'$$.identity_source', ''),
              tags = CASE
                WHEN JSON_TYPE(JSON_EXTRACT(payload, '$$.tags')) = 'ARRAY'
                THEN JSON_EXTRACT(payload, '$$.tags')
                ELSE NULL
              END,
              wps_device_name = NULLIF(payload->>'$$.wps_device_name', ''),
              wps_manufacturer = NULLIF(payload->>'$$.wps_manufacturer', ''),
              wps_model_name = NULLIF(payload->>'$$.wps_model_name', ''),
              device_fingerprint = NULLIF(payload->>'$$.device_fingerprint', ''),
              handshake_captured = COALESCE(CASE LOWER(NULLIF(payload->>'$$.handshake_captured', ''))
                WHEN 'true' THEN 1 WHEN 'false' THEN 0 WHEN '1' THEN 1 WHEN '0' THEN 0 ELSE NULL END, 0),
              updated_at = CURRENT_TIMESTAMP(6)
            WHERE dedupe_key = $dedupeKey
              AND stream_name = 'wireless.audit'
              AND payload_archived = 0
              AND NOT EXISTS (
                SELECT 1
                FROM sync_event_tombstones tombstone
                WHERE tombstone.dedupe_key = sync_events.dedupe_key
                  AND tombstone.stream_name = sync_events.stream_name
                  AND tombstone.expires_at > CURRENT_TIMESTAMP(6)
              )
              AND payload IS NOT NULL""".update.run

    project *>
      sql"""UPDATE sync_events
            SET wireless_search_text = NULLIF(LOWER(CONCAT_WS(
                  ' ', sensor_id, source_mac, bssid, destination_bssid, ssid,
                  wps_device_name, wps_manufacturer, wps_model_name,
                  device_fingerprint, app_protocol, src_ip, dst_ip, username
                )), '')
            WHERE dedupe_key = $dedupeKey
              AND stream_name = 'wireless.audit'
              AND payload_archived = 0
              AND NOT EXISTS (
                SELECT 1
                FROM sync_event_tombstones tombstone
                WHERE tombstone.dedupe_key = sync_events.dedupe_key
                  AND tombstone.stream_name = sync_events.stream_name
                  AND tombstone.expires_at > CURRENT_TIMESTAMP(6)
              )
              AND payload IS NOT NULL""".update.run

  def claimOutbox(
      ownerId: String,
      destinationTopics: List[String],
      leaseSeconds: Int
  ): IO[Either[DatabaseError, Option[OutboxRecord]]] =
    runDb("tidb.claim_outbox") {
      if ownerId.isBlank || destinationTopics.isEmpty then none[OutboxRecord].pure[ConnectionIO]
      else
        val token = UUID.randomUUID().toString
        val topics = destinationTopics.distinct.map(value => fr0"$value").intercalate(fr",")
        val safeLeaseSeconds = leaseSeconds.max(1)

        for
          updated <- (fr"""UPDATE outbox_events
                            SET status = 'leased',
                                owner_id = $ownerId,
                                lease_token = $token,
                                fence = fence + 1,
                                attempt_count = attempt_count + 1,
                                lease_expires_at = TIMESTAMPADD(SECOND, $safeLeaseSeconds, CURRENT_TIMESTAMP(6)),
                                updated_at = CURRENT_TIMESTAMP(6)
                            WHERE destination_topic IN (""" ++ topics ++ fr""" )
                              AND status = 'pending'
                              AND next_attempt_at <= CURRENT_TIMESTAMP(6)
                              AND (lease_expires_at IS NULL OR lease_expires_at <= CURRENT_TIMESTAMP(6))
                              AND attempt_count < max_attempts
                            ORDER BY created_at, outbox_id
                            LIMIT 1""").update.run
          claimed <- if updated == 1 then
            sql"""SELECT outbox_id, destination_topic, message_key, CAST(payload AS CHAR),
                         attempt_count, max_attempts, owner_id, lease_token, fence
                  FROM outbox_events
                  WHERE owner_id = $ownerId
                    AND lease_token = $token
                    AND status = 'leased'"""
              .query[(String, String, String, String, Int, Int, String, String, Long)]
              .unique
              .map { case (id, topic, key, payload, attempts, maxAttempts, owner, leaseToken, fence) =>
                Some(OutboxRecord(id, topic, key, payload, attempts, maxAttempts, LeaseIdentity(owner, leaseToken, fence)))
              }
          else none[OutboxRecord].pure[ConnectionIO]
        yield claimed
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
    for
      updated <- sql"""UPDATE outbox_events
                       SET status = 'published',
                           published_at = CURRENT_TIMESTAMP(6),
                           owner_id = NULL,
                           lease_token = NULL,
                           lease_expires_at = NULL,
                           last_error = NULL,
                           updated_at = CURRENT_TIMESTAMP(6)
                       WHERE outbox_id = ${record.outboxId}
                         AND status = 'leased'
                         AND owner_id = ${record.lease.ownerId}
                         AND lease_token = ${record.lease.token}
                         AND fence = ${record.lease.fence}
                         AND lease_expires_at > CURRENT_TIMESTAMP(6)""".update.run
      _ <- if updated == 1 then
        sql"""INSERT INTO outbox_publish_attempts (
                outbox_id, attempt_no, status, error_text, attempted_at
              ) VALUES (
                ${record.outboxId}, ${record.attemptCount}, 'published', NULL, CURRENT_TIMESTAMP(6)
              ) ON DUPLICATE KEY UPDATE status = 'published', error_text = NULL,
                  attempted_at = CURRENT_TIMESTAMP(6)""".update.run.void
      else ().pure[ConnectionIO]
      _ <- if updated == 1 then loadBatchId.traverse_ { batchId =>
        sql"""UPDATE sync_batches
              SET status = 'dispatched',
                  attempt_count = attempt_count + 1,
                  outbox_id = ${record.outboxId},
                  updated_at = CURRENT_TIMESTAMP(6)
              WHERE batch_id = $batchId
                AND status IN ('pending', 'dispatched')""".update.run.void *>
          sql"""UPDATE sync_jobs j
                JOIN sync_batches b ON b.job_id = j.job_id
                SET j.status = 'running',
                    j.started_at = COALESCE(j.started_at, CURRENT_TIMESTAMP(6))
                WHERE b.batch_id = $batchId
                  AND j.status IN ('pending', 'running')""".update.run.void
      } else ().pure[ConnectionIO]
    yield updated == 1

  private def parkMalformedLoadOutbox(
      record: OutboxRecord,
      error: IllegalArgumentException
  ): ConnectionIO[Boolean] =
    val errorText = s"published load outbox payload was malformed: ${error.getMessage}"
    for
      updated <- sql"""UPDATE outbox_events
                       SET status = 'failed',
                           published_at = CURRENT_TIMESTAMP(6),
                           owner_id = NULL,
                           lease_token = NULL,
                           lease_expires_at = NULL,
                           last_error = $errorText,
                           updated_at = CURRENT_TIMESTAMP(6)
                       WHERE outbox_id = ${record.outboxId}
                         AND status = 'leased'
                         AND owner_id = ${record.lease.ownerId}
                         AND lease_token = ${record.lease.token}
                         AND fence = ${record.lease.fence}
                         AND lease_expires_at > CURRENT_TIMESTAMP(6)""".update.run
      _ <- if updated == 1 then
        sql"""INSERT INTO outbox_publish_attempts (
                outbox_id, attempt_no, status, error_text, attempted_at
              ) VALUES (
                ${record.outboxId}, ${record.attemptCount}, 'failed', $errorText, CURRENT_TIMESTAMP(6)
              ) ON DUPLICATE KEY UPDATE status = 'failed', error_text = VALUES(error_text),
                  attempted_at = CURRENT_TIMESTAMP(6)""".update.run.void
      else ().pure[ConnectionIO]
    yield updated == 1

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

      for
        updated <- sql"""UPDATE outbox_events
                         SET status = $status,
                             owner_id = NULL,
                             lease_token = NULL,
                             lease_expires_at = NULL,
                             next_attempt_at = TIMESTAMPADD(SECOND, $delaySeconds, CURRENT_TIMESTAMP(6)),
                             last_error = $errorText,
                             updated_at = CURRENT_TIMESTAMP(6)
                         WHERE outbox_id = ${record.outboxId}
                           AND status = 'leased'
                           AND owner_id = ${record.lease.ownerId}
                           AND lease_token = ${record.lease.token}
                           AND fence = ${record.lease.fence}""".update.run
        _ <- if updated == 1 then
          sql"""INSERT INTO outbox_publish_attempts (
                  outbox_id, attempt_no, status, error_text, attempted_at
                ) VALUES (
                  ${record.outboxId}, ${record.attemptCount}, $status, $errorText, CURRENT_TIMESTAMP(6)
                ) ON DUPLICATE KEY UPDATE status = VALUES(status), error_text = VALUES(error_text),
                    attempted_at = CURRENT_TIMESTAMP(6)""".update.run.void
        else FC.raiseError(IllegalStateException(s"lost outbox lease ${record.outboxId}"))
      yield disposition
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
      val dedupeKey = s"load:${load.batchId}:${load.attempt.max(1)}"
      validateBrokerMetadata(metadata) *>
        existingBrokerEvidence(metadata).flatMap {
          case Some(existing) => verifyBrokerEvidence(metadata, dedupeKey, existing)
          case None =>
            enqueueResultTx(result, load.attempt) *>
              persistBrokerEvidence(metadata, dedupeKey, IngestionDisposition.Processed)
        }
    }

  def recordResultWithEvidence(
      result: TidbResult,
      metadata: BrokerRecordMetadata
  ): IO[Either[DatabaseError, Unit]] =
    runDb("tidb.record_result_with_evidence") {
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
    }

  private def enqueueResultTx(result: TidbResult, attempt: Int): ConnectionIO[Unit] =
    val safeAttempt = attempt.max(1)
    val messageKey = s"${result.batchId}:$safeAttempt"
    val outboxId = stableUuid(s"outbox:sync.oracle.result:$messageKey")
    sql"""INSERT INTO outbox_events (
            outbox_id, source_type, source_id, event_type,
            destination_topic, message_key, payload, status,
            attempt_count, max_attempts, next_attempt_at, created_at, updated_at
          ) VALUES (
            $outboxId, 'sync_batch', ${result.batchId}, 'sync.load.result',
            'sync.oracle.result', $messageKey, ${result.asJson.noSpaces}, 'pending',
            0, 5, CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6)
          ) ON DUPLICATE KEY UPDATE outbox_id = outbox_events.outbox_id""".update.run.void

  private def applyResultTransition(result: TidbResult): ConnectionIO[Unit] =
    for
      batch <- sql"""SELECT b.job_id, b.stream_name, b.payload_ref,
                             b.cursor_start, b.cursor_end, b.batch_no,
                             b.attempt_count, b.max_attempts
                      FROM sync_batches b
                      WHERE b.batch_id = ${result.batchId}"""
        .query[(String, String, String, String, String, Int, Int, Int)]
        .option
      row <- batch match
        case Some(value) if value._1 == result.jobId => value.pure[ConnectionIO]
        case Some(value) => FC.raiseError(IllegalStateException(
          s"result job ${result.jobId} does not own batch ${result.batchId}; expected ${value._1}"
        ))
        case None => FC.raiseError(IllegalStateException(s"unknown result batch ${result.batchId}"))
      _ <- if result.status == "success" then completeSuccessfulResult(result, row._2, row._5)
           else completeFailedResult(result, row)
    yield ()

  private def completeSuccessfulResult(
      result: TidbResult,
      streamName: String,
      cursorEnd: String
  ): ConnectionIO[Unit] =
    for
      _ <- sql"""UPDATE sync_batches
                  SET status = 'completed',
                      row_count = ${result.rowCount},
                      checksum = ${result.checksum},
                      last_error = NULL,
                      updated_at = CURRENT_TIMESTAMP(6)
                  WHERE batch_id = ${result.batchId}
                    AND job_id = ${result.jobId}
                    AND status IN ('dispatched', 'running', 'completed')""".update.run
      _ <- sql"""UPDATE sync_jobs
                  SET status = 'completed',
                      finished_at = COALESCE(finished_at, CURRENT_TIMESTAMP(6))
                  WHERE job_id = ${result.jobId}
                    AND NOT EXISTS (
                      SELECT 1 FROM sync_batches
                      WHERE job_id = ${result.jobId}
                        AND status <> 'completed'
                    )""".update.run
      _ <- sql"""INSERT INTO sync_cursors (stream_name, cursor_value, updated_at)
                  VALUES ($streamName, $cursorEnd, CURRENT_TIMESTAMP(6))
                  ON DUPLICATE KEY UPDATE cursor_value = VALUES(cursor_value),
                      updated_at = CURRENT_TIMESTAMP(6)""".update.run
    yield ()

  private def completeFailedResult(
      result: TidbResult,
      batch: (String, String, String, String, String, Int, Int, Int)
  ): ConnectionIO[Unit] =
    val (jobId, streamName, payloadRef, cursorStart, cursorEnd, batchNo, attempts, maxAttempts) = batch
    val retry = result.retryable && attempts < maxAttempts

    if retry then
      val nextAttempt = attempts + 1
      val messageKey = s"${result.batchId}:$nextAttempt"
      val outboxId = stableUuid(s"outbox:sync.oracle.load:$messageKey")
      val load = SyncLoad(
        jobId,
        result.batchId,
        Some(batchNo),
        streamName,
        payloadRef,
        cursorStart,
        cursorEnd,
        nextAttempt
      )
      sql"""UPDATE sync_batches
            SET status = 'pending',
                last_error = ${result.errorText},
                updated_at = CURRENT_TIMESTAMP(6)
            WHERE batch_id = ${result.batchId}
              AND job_id = $jobId
              AND status IN ('dispatched', 'running', 'pending')""".update.run.void *>
        sql"""INSERT INTO outbox_events (
                outbox_id, source_type, source_id, event_type,
                destination_topic, message_key, payload, status,
                attempt_count, max_attempts, next_attempt_at, created_at, updated_at
              ) VALUES (
                $outboxId, 'sync_batch', ${result.batchId}, 'sync.load.requested',
                'sync.oracle.load', $messageKey, ${load.asJson.noSpaces}, 'pending',
                0, $maxAttempts, CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6)
              ) ON DUPLICATE KEY UPDATE outbox_id = outbox_events.outbox_id""".update.run.void
    else
      sql"""UPDATE sync_batches
            SET status = 'failed',
                last_error = ${result.errorText},
                updated_at = CURRENT_TIMESTAMP(6)
            WHERE batch_id = ${result.batchId}
              AND job_id = $jobId""".update.run.void *>
        sql"""UPDATE sync_jobs
              SET status = 'failed',
                  finished_at = COALESCE(finished_at, CURRENT_TIMESTAMP(6))
              WHERE job_id = $jobId""".update.run.void *>
        sql"""INSERT INTO sync_errors (job_id, batch_id, error_class, error_text)
              VALUES ($jobId, ${result.batchId}, ${result.errorClass}, ${result.errorText})""".update.run.void

  private def existingBrokerEvidence(
      metadata: BrokerRecordMetadata
  ): ConnectionIO[Option[(String, String, String)]] =
    sql"""SELECT payload_sha256, artifact_sha256, dedupe_key
          FROM ingestion_evidence
          WHERE group_id = ${metadata.consumerGroup}
            AND topic = ${metadata.topic}
            AND partition_id = ${metadata.partition}
            AND record_offset = ${metadata.offset}"""
      .query[(String, String, String)]
      .option

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
    sql"""INSERT INTO ingestion_evidence (
            topic, partition_id, record_offset, group_id, group_version,
            artifact_sha256, message_key, payload_sha256, disposition,
            dedupe_key, first_seen_at, updated_at
          ) VALUES (
            ${metadata.topic}, ${metadata.partition}, ${metadata.offset}, ${metadata.consumerGroup},
            ${metadata.groupVersion}, ${metadata.artifactSha256}, ${metadata.messageKey},
            ${metadata.payloadSha256}, ${disposition.databaseValue}, $dedupeKey,
            CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6)
          )""".update.run.void *>
      advanceConsumerOffset(metadata)

  private def advanceConsumerOffset(metadata: BrokerRecordMetadata): ConnectionIO[Unit] =
    val nextOffset = metadata.offset + 1L
    sql"""INSERT INTO consumer_offsets (
            group_id, topic, partition_id, next_offset, group_version,
            artifact_sha256, updated_at
          ) VALUES (
            ${metadata.consumerGroup}, ${metadata.topic}, ${metadata.partition}, $nextOffset,
            ${metadata.groupVersion}, ${metadata.artifactSha256}, CURRENT_TIMESTAMP(6)
          ) ON DUPLICATE KEY UPDATE
            next_offset = GREATEST(consumer_offsets.next_offset, VALUES(next_offset)),
            group_version = VALUES(group_version),
            artifact_sha256 = VALUES(artifact_sha256),
            updated_at = CURRENT_TIMESTAMP(6)""".update.run.void

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
      for
        parked <- sql"""UPDATE outbox_events
                         SET status = 'failed',
                             owner_id = NULL,
                             lease_token = NULL,
                             lease_expires_at = NULL,
                             last_error = 'publish lease expired; max attempts reached',
                             updated_at = CURRENT_TIMESTAMP(6)
                         WHERE status = 'leased'
                           AND lease_expires_at <= CURRENT_TIMESTAMP(6)
                           AND attempt_count >= max_attempts""".update.run
        retried <- sql"""UPDATE outbox_events
                          SET status = 'pending',
                              owner_id = NULL,
                              lease_token = NULL,
                              lease_expires_at = NULL,
                              next_attempt_at = CURRENT_TIMESTAMP(6),
                              last_error = 'publish lease expired; retrying',
                              updated_at = CURRENT_TIMESTAMP(6)
                          WHERE status = 'leased'
                            AND lease_expires_at <= CURRENT_TIMESTAMP(6)
                            AND attempt_count < max_attempts""".update.run
      yield parked + retried
    }

  def ensureCursor(streamName: String): IO[Either[DatabaseError, String]] =
    runDb("tidb.ensure_cursor") {
      for
        _ <- sql"""INSERT INTO sync_cursors (stream_name, cursor_value, updated_at)
                   VALUES ($streamName, '0', NOW(6))
                   ON DUPLICATE KEY UPDATE updated_at = NOW(6)""".update.run
        cursor <- sql"""SELECT cursor_value FROM sync_cursors WHERE stream_name = $streamName"""
          .query[String].unique
      yield cursor
    }

  def ensureAllCursors(streamNames: List[String], dbSemaphore: Semaphore[IO]): IO[Either[DatabaseError, String]] =
    if streamNames.isEmpty then IO.pure(Right("ok"))
    else
      dbSemaphore.available.flatMap { available =>
        val parallelism = available.toInt.max(1)
        streamNames.parTraverseN(parallelism) { name =>
          dbSemaphore.permit.use { _ =>
            runDb(s"tidb.ensure_cursor_$name") {
              sql"""INSERT INTO sync_cursors (stream_name, cursor_value, updated_at)
                    VALUES ($name, '0', NOW(6))
                    ON DUPLICATE KEY UPDATE updated_at = NOW(6)""".update.run
            }
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

  def generateShadowAlerts(): IO[Either[DatabaseError, List[String]]] =
    runDb("tidb.generate_shadow_alerts") {
      (fr"""INSERT INTO wireless_shadow_alerts (
            source_mac, first_occurred_at, last_occurred_at, occurrence_count,
            destination_bssid, ssid, sensor_id, location_id, signal_dbm,
            reason, evidence, created_at, updated_at
          )
          SELECT
            w.source_mac, w.observed_at, w.observed_at, 1,
            w.destination_bssid, w.ssid, w.sensor_id, w.location_id, w.signal_dbm,
            'strong_wireless_without_proxy_presence',
            JSON_OBJECT('window_seconds', 60, 'signal_threshold_dbm', -50, 'presence_window_seconds', 300),
            NOW(6), NOW(6)
          FROM (
            SELECT DISTINCT
              LOWER(e.payload->>'$$.source_mac') AS source_mac,
              e.observed_at,
              LOWER(COALESCE(NULLIF(TRIM(e.payload->>'$$.destination_bssid'), ''), NULLIF(TRIM(e.payload->>'$$.bssid'), ''))) AS destination_bssid,
              e.payload->>'$$.ssid' AS ssid,
              e.payload->>'$$.sensor_id' AS sensor_id,
              e.payload->>'$$.location_id' AS location_id,
              CAST(e.payload->>'$$.signal_dbm' AS SIGNED) AS signal_dbm
            FROM sync_events e
            WHERE e.stream_name = 'wireless.audit'
              AND e.observed_at >= NOW(6) - """ ++ Fragment.const(s"INTERVAL $windowSecs SECOND") ++
            fr""" AND e.payload IS NOT NULL
          ) w
          WHERE w.source_mac IS NOT NULL
            AND w.source_mac REGEXP '^[0-9a-f]{2}(:[0-9a-f]{2}){5}$$'
            AND w.signal_dbm >= $signalThreshold
            AND NOT EXISTS (
              SELECT 1 FROM wireless_authorized_networks awn
              WHERE awn.enabled = TRUE
                AND (awn.location_id IS NULL OR awn.location_id = w.location_id)
                AND (awn.ssid IS NULL OR (w.ssid IS NOT NULL AND awn.ssid = w.ssid))
                AND (awn.bssid IS NULL OR (w.destination_bssid IS NOT NULL AND awn.bssid = w.destination_bssid))
            )
            AND NOT EXISTS (
              SELECT 1 FROM devices d
              WHERE d.mac_id = w.source_mac AND d.last_seen >= NOW(6) - """ ++ Fragment.const(s"INTERVAL $presenceWindowSecs SECOND") ++
            fr"""
          )
          ON DUPLICATE KEY UPDATE
            last_occurred_at = GREATEST(wireless_shadow_alerts.last_occurred_at, VALUES(last_occurred_at)),
            occurrence_count = wireless_shadow_alerts.occurrence_count + 1,
            destination_bssid = IF(VALUES(last_occurred_at) >= wireless_shadow_alerts.last_occurred_at, VALUES(destination_bssid), wireless_shadow_alerts.destination_bssid),
            ssid = IF(VALUES(last_occurred_at) >= wireless_shadow_alerts.last_occurred_at, VALUES(ssid), wireless_shadow_alerts.ssid),
            sensor_id = IF(VALUES(last_occurred_at) >= wireless_shadow_alerts.last_occurred_at, VALUES(sensor_id), wireless_shadow_alerts.sensor_id),
            location_id = IF(VALUES(last_occurred_at) >= wireless_shadow_alerts.last_occurred_at, VALUES(location_id), wireless_shadow_alerts.location_id),
            signal_dbm = IF(VALUES(last_occurred_at) >= wireless_shadow_alerts.last_occurred_at, VALUES(signal_dbm), wireless_shadow_alerts.signal_dbm),
            updated_at = NOW(6)""").update.run *>
        (fr"""SELECT JSON_OBJECT(
              'event_type', 'shadow_device',
              'first_occurred_at', first_occurred_at,
              'last_occurred_at', last_occurred_at,
              'source_mac', source_mac,
              'occurrence_count', occurrence_count,
              'destination_bssid', destination_bssid,
              'ssid', ssid,
              'sensor_id', sensor_id,
              'location_id', location_id,
              'signal_dbm', signal_dbm,
              'reason', reason,
              'evidence', evidence
            ) AS alert_json
            FROM wireless_shadow_alerts
            WHERE updated_at >= NOW(6) - INTERVAL 2 SECOND""").query[String].to[List]
    }

  def lookupDeviceByMac(mac: String): IO[Either[DatabaseError, Option[String]]] =
    runDb("tidb.lookup_device_by_mac") {
      sql"""SELECT JSON_OBJECT(
              'device_id', mac_id,
              'username', username,
              'display_name', display_name,
              'hostname', hostname
            ) AS device_json
            FROM devices
            WHERE LOWER(mac_id) = LOWER($mac)
            LIMIT 1"""
        .query[String]
        .option
    }

  def listAuthorizedNetworks(): IO[Either[DatabaseError, String]] =
    runDb("tidb.list_authorized_networks") {
      sql"""SELECT COALESCE(
              JSON_ARRAYAGG(
                JSON_OBJECT(
                  'ssid', ssid,
                  'bssid', LOWER(bssid),
                  'location_id', location_id,
                  'label', label,
                  'enabled', enabled
                )
              ),
              '[]'
            ) AS networks_json
            FROM wireless_authorized_networks
            WHERE enabled = TRUE""".query[String].unique
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
          "batch_id" -> batchId, "probe_count" -> probes.length.toString, "payload_bytes" -> probesJson.length.toString)

        probes.traverse { probe =>
          val ssid = probe.hcursor.get[String]("ssid").getOrElse("")
          val clientMac = probe.hcursor.get[String]("client_mac").getOrElse("")
          val firstSeen = probe.hcursor.get[String]("first_seen").toOption.flatMap(parseTs)
          val lastSeen = probe.hcursor.get[String]("last_seen").toOption.flatMap(parseTs)
          val probeCount = probe.hcursor.get[Long]("probe_count").getOrElse(1L)
          val locationId = probe.hcursor.get[String]("location_id").toOption
          val observedBssid = probe.hcursor.get[String]("observed_bssid").toOption
            .orElse(probe.hcursor.get[String]("known_bssid").toOption)
            .orElse(probe.hcursor.get[String]("bssid").toOption)

          sql"""INSERT INTO wireless_clients (ssid, client_mac, known_bssid, first_seen, last_seen,
                  probe_count, location_id, last_probe_batch_id)
                VALUES ($ssid, $clientMac,
                  (SELECT MAX(authorized.bssid) FROM wireless_authorized_networks authorized
                   WHERE LOWER(authorized.ssid) = LOWER($ssid) AND authorized.enabled = TRUE
                     AND ($observedBssid IS NULL OR LOWER(authorized.bssid) = LOWER($observedBssid))
                     AND ($locationId IS NULL OR authorized.location_id = $locationId)
                   HAVING COUNT(*) = 1),
                  $firstSeen, $lastSeen, $probeCount,
                  $locationId, $batchId)
                ON DUPLICATE KEY UPDATE
                  first_seen = LEAST(wireless_clients.first_seen, VALUES(first_seen)),
                  last_seen = GREATEST(wireless_clients.last_seen, VALUES(last_seen)),
                  probe_count = CASE
                    WHEN wireless_clients.last_probe_batch_id IS NULL
                      OR wireless_clients.last_probe_batch_id != VALUES(last_probe_batch_id)
                    THEN wireless_clients.probe_count + VALUES(probe_count)
                    ELSE wireless_clients.probe_count
                  END,
                  known_bssid = COALESCE(VALUES(known_bssid), wireless_clients.known_bssid),
                  location_id = COALESCE(VALUES(location_id), wireless_clients.location_id),
                  last_probe_batch_id = VALUES(last_probe_batch_id)""".update.run
        }.map { affectedPerProbe =>
          val totalAffected = affectedPerProbe.sum
          log.info("probe_flush_batch", "status" -> "inserted",
            "batch_id" -> batchId, "total_affected_rows" -> totalAffected.toString)
          totalAffected
        }
    }

  private def runDb[A](operation: String)(fa: ConnectionIO[A]): IO[Either[DatabaseError, A]] =
    TidbRepository.retryTransient(operation)(fa.transact(xa))
      .map(Right(_))
      .handleError { cause =>
        log.error("db_error", cause, "operation" -> operation)
        TidbErrorClass.classify(cause) match
          case TidbErrorClass.Retryable => Left(DatabaseError.Retryable(operation, cause, cause.getMessage))
          case TidbErrorClass.Permanent => Left(DatabaseError.Permanent(operation, cause, cause.getMessage))
      }

  private def stableUuid(value: String): String =
    UUID.nameUUIDFromBytes(value.getBytes(StandardCharsets.UTF_8)).toString

  private def parseTs(s: String): Option[java.sql.Timestamp] =
    try
      Some(java.sql.Timestamp.from(java.time.Instant.parse(s)))
    catch case _: Exception =>
      try
        Some(java.sql.Timestamp.valueOf(s.replace("T", " ").substring(0, 19)))
      catch case _: Exception => None

object TidbRepository:
  private val log = StructuredLogger(getClass)
  private val transactionRetryMaxAttempts = 5
  private val transactionRetryBaseDelay = 25.millis

  private[tidb] def retryTransient[A](operation: String)(fa: IO[A]): IO[A] =
    def loop(attempt: Int): IO[A] =
      fa.handleErrorWith { cause =>
        if attempt < transactionRetryMaxAttempts &&
            TidbErrorClass.classify(cause) == TidbErrorClass.Retryable
        then
          val delay = transactionRetryBaseDelay * (1L << (attempt - 1))
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
