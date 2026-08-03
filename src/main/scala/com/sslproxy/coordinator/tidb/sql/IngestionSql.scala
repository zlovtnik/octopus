package com.sslproxy.coordinator.tidb.sql

import com.sslproxy.coordinator.domain.{BrokerRecordMetadata, IngestionDisposition, ResolvedScanRequestRecord}
import com.sslproxy.coordinator.tidb.SyncEventHydrationCandidate
import doobie.{Fragment, Query0, Update0}
import doobie.implicits.*

object IngestionSql:
  val ConnectivityQuery: Query0[Int] = sql"SELECT 1".query[Int]

  val PendingLedgerCountQuery: Query0[Long] =
    sql"""SELECT COUNT(*) FROM sync_events
           WHERE status IN ('pending', 'processing')""".query[Long]

  def consumerPartitions(groupId: String, topic: String): Query0[Int] =
    sql"""SELECT partition_id FROM consumer_offsets
           WHERE group_id = $groupId AND topic = $topic""".query[Int]

  def hydrationCandidates(
      after: Option[SyncEventHydrationCandidate],
      limit: Int
  ): Query0[(String, String, java.sql.Timestamp, String, Option[String], Option[String])] =
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
                  CAST(e.payload AS CHAR), e.payload_sha256
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
      .query[(String, String, java.sql.Timestamp, String, Option[String], Option[String])]

  def existingEvidence(
      metadata: BrokerRecordMetadata
  ): Query0[(String, String, String)] =
    sql"""SELECT payload_sha256, artifact_sha256, dedupe_key
           FROM ingestion_evidence
           WHERE topic = ${metadata.topic}
             AND partition_id = ${metadata.partition}
             AND record_offset = ${metadata.offset}
             AND group_id = ${metadata.consumerGroup}"""
      .query[(String, String, String)]

  def persistEvidence(
      metadata: BrokerRecordMetadata,
      payloadSha256: String,
      dedupeKey: String,
      disposition: IngestionDisposition
  ): Update0 =
    sql"""INSERT INTO ingestion_evidence (
             topic, partition_id, record_offset, group_id, group_version,
             artifact_sha256, message_key, payload_sha256, disposition,
             dedupe_key, first_seen_at, updated_at
           ) VALUES (
             ${metadata.topic}, ${metadata.partition}, ${metadata.offset}, ${metadata.consumerGroup},
             ${metadata.groupVersion}, ${metadata.artifactSha256}, ${metadata.messageKey}, $payloadSha256,
             ${disposition.databaseValue}, $dedupeKey, CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6)
           ) ON DUPLICATE KEY UPDATE
             updated_at = CURRENT_TIMESTAMP(6)""".update

  def activeTombstone(streamName: String, dedupeKey: String): Query0[Int] =
    sql"""SELECT EXISTS(
             SELECT 1 FROM sync_event_tombstones
             WHERE stream_name = $streamName
               AND dedupe_key = $dedupeKey
               AND expires_at > CURRENT_TIMESTAMP(6)
           )""".query[Int]

  def syncEventExists(streamName: String, dedupeKey: String): Query0[Int] =
    sql"""SELECT EXISTS(
             SELECT 1 FROM sync_events
             WHERE dedupe_key = $dedupeKey
               AND stream_name = $streamName
           )""".query[Int]

  def insertSyncEvent(
      record: ResolvedScanRequestRecord,
      observedAt: java.sql.Timestamp,
      eventKind: Option[String]
  ): Update0 =
    val payload = Option(record.payloadJson)
    sql"""INSERT INTO sync_events (
             dedupe_key, stream_name, observed_at, payload_ref, payload,
             payload_sha256, status, attempt_count, last_error,
             producer, event_kind, created_at, updated_at
           ) VALUES (
             ${record.dedupeKey}, ${record.streamName}, $observedAt, ${record.payloadRef}, $payload,
             ${record.eventPayloadSha256}, 'batched', 1, NULL,
             'ssl-proxy', $eventKind, CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6)
           ) ON DUPLICATE KEY UPDATE dedupe_key = sync_events.dedupe_key""".update

  def insertJob(jobId: String, streamName: String, dedupeKey: String): Update0 =
    sql"""INSERT INTO sync_jobs (
             job_id, stream_name, dedupe_key, status, attempt_count, created_at
           ) VALUES (
             $jobId, $streamName, $dedupeKey, 'pending', 0, CURRENT_TIMESTAMP(6)
           ) ON DUPLICATE KEY UPDATE job_id = sync_jobs.job_id""".update

  def insertBatch(
      batchId: String,
      jobId: String,
      record: ResolvedScanRequestRecord,
      cursorStart: String,
      cursorEnd: String
  ): Update0 =
    sql"""INSERT INTO sync_batches (
             batch_id, job_id, batch_no, payload_ref, status, row_count,
             attempt_count, dedupe_key, stream_name, cursor_start, cursor_end,
             created_at, updated_at
           ) VALUES (
             $batchId, $jobId, 0, ${record.payloadRef}, 'pending', 1,
             0, ${record.dedupeKey}, ${record.streamName}, $cursorStart, $cursorEnd,
             CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6)
           ) ON DUPLICATE KEY UPDATE batch_id = sync_batches.batch_id""".update

  def hydrateEvent(
      streamName: String,
      dedupeKey: String,
      payloadJson: String,
      payloadSha256: String,
      eventKind: Option[String]
  ): Update0 =
    sql"""UPDATE sync_events
           SET payload = $payloadJson,
               payload_sha256 = $payloadSha256,
               event_kind = COALESCE($eventKind, event_kind),
               updated_at = CURRENT_TIMESTAMP(6)
           WHERE dedupe_key = $dedupeKey
             AND stream_name = $streamName
             AND payload_archived = 0
             AND (payload IS NULL OR payload_sha256 = $payloadSha256)
             AND NOT EXISTS (
               SELECT 1
               FROM sync_event_tombstones tombstone
               WHERE tombstone.dedupe_key = sync_events.dedupe_key
                 AND tombstone.stream_name = sync_events.stream_name
                 AND tombstone.expires_at > CURRENT_TIMESTAMP(6)
             )""".update

  def advanceConsumerOffset(metadata: BrokerRecordMetadata): Update0 =
    val nextOffset = metadata.offset + 1L
    sql"""INSERT INTO consumer_offsets (
             group_id, topic, partition_id, next_offset, group_version,
             artifact_sha256, updated_at
           ) VALUES (
             ${metadata.consumerGroup}, ${metadata.topic}, ${metadata.partition}, $nextOffset,
             ${metadata.groupVersion}, ${metadata.artifactSha256}, CURRENT_TIMESTAMP(6)
           ) ON DUPLICATE KEY UPDATE
             group_version = CASE
               WHEN VALUES(next_offset) > consumer_offsets.next_offset THEN VALUES(group_version)
               ELSE consumer_offsets.group_version
             END,
             artifact_sha256 = CASE
               WHEN VALUES(next_offset) > consumer_offsets.next_offset THEN VALUES(artifact_sha256)
               ELSE consumer_offsets.artifact_sha256
             END,
             next_offset = GREATEST(consumer_offsets.next_offset, VALUES(next_offset)),
             updated_at = CURRENT_TIMESTAMP(6)""".update

  def advanceCursor(streamName: String, cursorEnd: String): Update0 =
    sql"""INSERT INTO sync_cursors (stream_name, cursor_value, updated_at)
           VALUES ($streamName, $cursorEnd, CURRENT_TIMESTAMP(6))
           ON DUPLICATE KEY UPDATE
             cursor_value = CASE
               WHEN VALUES(cursor_value) REGEXP '^[0-9]+$$'
                 AND sync_cursors.cursor_value REGEXP '^[0-9]+$$'
                 AND CAST(VALUES(cursor_value) AS DECIMAL(65, 0)) >
                     CAST(sync_cursors.cursor_value AS DECIMAL(65, 0))
               THEN VALUES(cursor_value)
               WHEN NOT (VALUES(cursor_value) REGEXP '^[0-9]+$$')
                 AND NOT (sync_cursors.cursor_value REGEXP '^[0-9]+$$')
                 AND VALUES(cursor_value) > sync_cursors.cursor_value
               THEN VALUES(cursor_value)
               ELSE sync_cursors.cursor_value
             END,
             updated_at = CURRENT_TIMESTAMP(6)""".update

  def ensureCursor(streamName: String): Update0 =
    sql"""INSERT INTO sync_cursors (stream_name, cursor_value, updated_at)
           VALUES ($streamName, '0', CURRENT_TIMESTAMP(6))
           ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP(6)""".update

  def cursor(streamName: String): Query0[String] =
    sql"SELECT cursor_value FROM sync_cursors WHERE stream_name = $streamName".query[String]
