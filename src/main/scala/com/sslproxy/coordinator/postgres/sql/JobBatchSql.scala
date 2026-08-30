package com.sslproxy.coordinator.postgres.sql

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
               AND (e.status = 'pending' OR e.updated_at <= (CURRENT_TIMESTAMP + (-$backoffSeconds) * INTERVAL '1 second'))
             ORDER BY e.observed_at, e.stream_name, e.dedupe_key
             LIMIT $limit
             FOR UPDATE""")
          .query[(String, String)]
          .to[List]

      candidateQuery.flatMap {
        case Nil => 0L.pure[ConnectionIO]
        case selected =>
          val selectedKeys = selected
            .map { case (streamName, dedupeKey) =>
              fr0"($streamName, $dedupeKey)"
            }
            .intercalate(fr",")
          val selectedPredicate =
            fr0"(e.stream_name, e.dedupe_key) IN (" ++ selectedKeys ++ fr0")"

          val insertJobs =
            fr"""INSERT INTO sync_jobs (
                   job_id, stream_name, dedupe_key, status, attempt_count, created_at
                 )
                 SELECT gen_random_uuid(), e.stream_name, e.dedupe_key, 'pending', 0, CURRENT_TIMESTAMP
                 FROM sync_events e
                 WHERE """ ++ selectedPredicate ++
              fr""" ON CONFLICT (dedupe_key, stream_name) DO UPDATE SET job_id = sync_jobs.job_id"""

          val insertBatches =
            fr"""INSERT INTO sync_batches (
                   batch_id, job_id, batch_no, payload_ref, status, row_count,
                   attempt_count, dedupe_key, stream_name, cursor_start, cursor_end,
                   created_at, updated_at
                 )
                 SELECT gen_random_uuid(), j.job_id, 0, e.payload_ref, 'pending', 1,
                        0, e.dedupe_key, e.stream_name,
                        COALESCE(c.cursor_value, '0'),
                        CASE
                          WHEN e.stream_name = 'wireless.audit'
                          THEN CAST(FLOOR(EXTRACT(EPOCH FROM e.observed_at)) AS TEXT)
                          ELSE e.dedupe_key
                        END,
                        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                 FROM sync_events e
                 JOIN sync_jobs j
                   ON j.stream_name = e.stream_name AND j.dedupe_key = e.dedupe_key
                 LEFT JOIN sync_cursors c ON c.stream_name = e.stream_name
                 WHERE """ ++ selectedPredicate ++
              fr""" ON CONFLICT (dedupe_key, stream_name) DO UPDATE SET
                   batch_id = sync_batches.batch_id,
                   updated_at = CURRENT_TIMESTAMP"""

          for
            _ <- insertJobs.update.run
            _ <- insertBatches.update.run
            processed <- (fr"""UPDATE sync_events e
                                SET status = 'batched',
                                    attempt_count = e.attempt_count + 1,
                                    last_error = NULL,
                                    updated_at = CURRENT_TIMESTAMP
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
             SELECT gen_random_uuid(), 'sync_batch', b.batch_id, 'sync.load.requested',
                    'sync.oracle.load', CONCAT(b.batch_id, ':', b.attempt_count + 1),
                    jsonb_build_object(
                      'job_id', b.job_id,
                      'batch_id', b.batch_id,
                      'batch_no', b.batch_no,
                      'stream_name', b.stream_name,
                      'payload_ref', b.payload_ref,
                      'cursor_start', b.cursor_start,
                      'cursor_end', b.cursor_end,
                      'attempt', b.attempt_count + 1
                    ),
                    'pending', 0, $attempts, CURRENT_TIMESTAMP,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
             FROM sync_batches b
             WHERE b.status = 'pending'
               AND b.stream_name IN (""" ++ streams ++ fr""" )
             ORDER BY b.created_at, b.batch_id
             LIMIT $batchLimit
             ON CONFLICT (destination_topic, message_key) DO UPDATE SET
               payload = EXCLUDED.payload,
               attempt_count = CASE WHEN outbox_events.status IN ('published', 'failed', 'cancelled') THEN 0 ELSE outbox_events.attempt_count END,
               max_attempts = EXCLUDED.max_attempts,
               next_attempt_at = CASE WHEN outbox_events.status IN ('published', 'failed', 'cancelled') THEN CURRENT_TIMESTAMP ELSE outbox_events.next_attempt_at END,
               owner_id = CASE WHEN outbox_events.status IN ('published', 'failed', 'cancelled') THEN NULL ELSE outbox_events.owner_id END,
               lease_token = CASE WHEN outbox_events.status IN ('published', 'failed', 'cancelled') THEN NULL ELSE outbox_events.lease_token END,
               lease_expires_at = CASE WHEN outbox_events.status IN ('published', 'failed', 'cancelled') THEN NULL ELSE outbox_events.lease_expires_at END,
               published_at = CASE WHEN outbox_events.status IN ('published', 'failed', 'cancelled') THEN NULL ELSE outbox_events.published_at END,
               last_error = CASE WHEN outbox_events.status IN ('published', 'failed', 'cancelled') THEN NULL ELSE outbox_events.last_error END,
               status = CASE WHEN outbox_events.status IN ('published', 'failed', 'cancelled') THEN 'pending' ELSE outbox_events.status END,
               updated_at = CURRENT_TIMESTAMP""").update
    }
