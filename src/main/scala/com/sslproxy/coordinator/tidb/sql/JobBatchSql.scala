package com.sslproxy.coordinator.tidb.sql

import cats.syntax.all.*
import doobie.{ConnectionIO, Update0}
import doobie.implicits.*

object JobBatchSql:
  def processIngestLedger(
      streamNames: List[String],
      scanMaxAttempts: Int,
      scanRetryBackoffSeconds: Int,
      ingestBatchSize: Int
  ): ConnectionIO[Long] =
    val normalizedStreams = streamNames.map(_.trim).filter(_.nonEmpty).distinct
    val limit = ingestBatchSize.max(1)
    val backoffSeconds = scanRetryBackoffSeconds.max(1)
    val maxAttempts = scanMaxAttempts.max(1)

    if normalizedStreams.isEmpty then 0L.pure[ConnectionIO]
    else
      val streamClause = normalizedStreams.map(value => fr0"$value").intercalate(fr",")
      val candidateQuery =
        (fr"""SELECT e.stream_name, e.dedupe_key
             FROM sync_events e
             WHERE e.stream_name IN (""" ++ streamClause ++ fr""" )
               AND e.status IN ('pending', 'failed')
               AND e.attempt_count < $maxAttempts
               AND (e.status = 'pending' OR e.updated_at <= TIMESTAMPADD(SECOND, -$backoffSeconds, CURRENT_TIMESTAMP(6)))
             ORDER BY e.observed_at, e.stream_name, e.dedupe_key
             LIMIT $limit
             FOR UPDATE""")
          .query[(String, String)]
          .to[List]

      candidateQuery.flatMap {
        case Nil => 0L.pure[ConnectionIO]
        case selected =>
          val selectedKeys = selected.map { case (streamName, dedupeKey) =>
            fr0"($streamName, $dedupeKey)"
          }.intercalate(fr",")
          val selectedPredicate =
            fr0"(e.stream_name, e.dedupe_key) IN (" ++ selectedKeys ++ fr0")"

          val insertJobs =
            fr"""INSERT INTO sync_jobs (
                   job_id, stream_name, dedupe_key, status, attempt_count, created_at
                 )
                 SELECT UUID(), e.stream_name, e.dedupe_key, 'pending', 0, CURRENT_TIMESTAMP(6)
                 FROM sync_events e
                 WHERE """ ++ selectedPredicate ++
              fr""" ON DUPLICATE KEY UPDATE job_id = sync_jobs.job_id"""

          val insertBatches =
            fr"""INSERT INTO sync_batches (
                   batch_id, job_id, batch_no, payload_ref, status, row_count,
                   attempt_count, dedupe_key, stream_name, cursor_start, cursor_end,
                   created_at, updated_at
                 )
                 SELECT UUID(), j.job_id, 0, e.payload_ref, 'pending', 1,
                        0, e.dedupe_key, e.stream_name,
                        COALESCE(c.cursor_value, '0'),
                        CASE
                          WHEN e.stream_name = 'wireless.audit'
                          THEN CAST(FLOOR(UNIX_TIMESTAMP(e.observed_at)) AS CHAR)
                          ELSE e.dedupe_key
                        END,
                        CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6)
                 FROM sync_events e
                 JOIN sync_jobs j
                   ON j.stream_name = e.stream_name AND j.dedupe_key = e.dedupe_key
                 LEFT JOIN sync_cursors c ON c.stream_name = e.stream_name
                 WHERE """ ++ selectedPredicate ++
              fr""" ON DUPLICATE KEY UPDATE
                   batch_id = sync_batches.batch_id,
                   updated_at = CURRENT_TIMESTAMP(6)"""

          for
            _ <- insertJobs.update.run
            _ <- insertBatches.update.run
            processed <- (fr"""UPDATE sync_events e
                                SET e.status = 'batched',
                                    e.attempt_count = e.attempt_count + 1,
                                    e.last_error = NULL,
                                    e.updated_at = CURRENT_TIMESTAMP(6)
                                WHERE """ ++ selectedPredicate ++ fr"""
                                  AND e.status IN ('pending', 'failed')
                                  AND EXISTS (
                                    SELECT 1 FROM sync_batches b
                                    WHERE b.stream_name = e.stream_name
                                      AND b.dedupe_key = e.dedupe_key
                                      AND b.status = 'pending'
                                      AND b.created_at >= e.updated_at
                                  )""").update.run
          yield processed.toLong
      }

  def prepareLoadDispatch(
      streamNames: List[String],
      maxAttempts: Int,
      limit: Int
  ): Option[Update0] =
    val normalized = streamNames.map(_.trim).filter(_.nonEmpty).distinct
    Option.when(normalized.nonEmpty) {
      val streams = normalized.map(value => fr0"$value").intercalate(fr",")
      val attempts = maxAttempts.max(1)
      val batchLimit = limit.max(1)
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
                    'pending', 0, $attempts, CURRENT_TIMESTAMP(6),
                    CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6)
             FROM sync_batches b
             WHERE b.status = 'pending'
               AND b.stream_name IN (""" ++ streams ++ fr""" )
             ORDER BY b.created_at, b.batch_id
             LIMIT $batchLimit
             ON DUPLICATE KEY UPDATE
               payload = VALUES(payload),
               attempt_count = IF(status IN ('published', 'failed', 'cancelled'), 0, attempt_count),
               max_attempts = VALUES(max_attempts),
               next_attempt_at = IF(status IN ('published', 'failed', 'cancelled'), CURRENT_TIMESTAMP(6), next_attempt_at),
               owner_id = IF(status IN ('published', 'failed', 'cancelled'), NULL, owner_id),
               lease_token = IF(status IN ('published', 'failed', 'cancelled'), NULL, lease_token),
               lease_expires_at = IF(status IN ('published', 'failed', 'cancelled'), NULL, lease_expires_at),
               published_at = IF(status IN ('published', 'failed', 'cancelled'), NULL, published_at),
               last_error = IF(status IN ('published', 'failed', 'cancelled'), NULL, last_error),
               status = IF(status IN ('published', 'failed', 'cancelled'), 'pending', status),
               updated_at = CURRENT_TIMESTAMP(6)""").update
    }
