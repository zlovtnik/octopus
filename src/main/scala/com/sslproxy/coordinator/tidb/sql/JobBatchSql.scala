package com.sslproxy.coordinator.tidb.sql

import cats.syntax.all.*
import doobie.Update0
import doobie.implicits.*

object JobBatchSql:
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
               updated_at = CURRENT_TIMESTAMP(6)""").update
    }
