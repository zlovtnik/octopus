package com.sslproxy.coordinator.tidb.sql

import cats.syntax.all.*
import com.sslproxy.coordinator.tidb.TidbResult
import doobie.{ConnectionIO, Query0}
import doobie.implicits.*
import io.circe.syntax.*

object ResultSql:
  final case class BatchState(
      jobId: String,
      streamName: String,
      payloadRef: String,
      cursorStart: String,
      cursorEnd: String,
      batchNo: Int,
      attemptCount: Int,
      maxAttempts: Int
  )

  def enqueue(result: TidbResult, attempt: Int, outboxId: String): ConnectionIO[Unit] =
    val safeAttempt = attempt.max(1)
    val messageKey = s"${result.batchId}:$safeAttempt"
    sql"""INSERT INTO outbox_events (
             outbox_id, source_type, source_id, event_type,
             destination_topic, message_key, payload, status,
             attempt_count, max_attempts, next_attempt_at, created_at, updated_at
           ) VALUES (
             $outboxId, 'sync_batch', ${result.batchId}, 'sync.load.result',
             'sync.oracle.result', $messageKey, ${result.asJson.noSpaces}, 'pending',
             0, 5, CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6)
           ) ON DUPLICATE KEY UPDATE outbox_id = outbox_events.outbox_id""".update.run.void

  def batchForUpdate(batchId: String): Query0[BatchState] =
    sql"""SELECT b.job_id, b.stream_name, b.payload_ref,
                  b.cursor_start, b.cursor_end, b.batch_no,
                  b.attempt_count, b.max_attempts
           FROM sync_batches b
           WHERE b.batch_id = $batchId
           FOR UPDATE"""
      .query[(String, String, String, String, String, Int, Int, Int)]
      .map(BatchState.apply.tupled)

  def completeSuccessful(
      result: TidbResult,
      streamName: String,
      cursorEnd: String
  ): ConnectionIO[Unit] =
    for
      updated <- sql"""UPDATE sync_batches
                         SET status = 'completed',
                             row_count = ${result.rowCount},
                             checksum = ${result.checksum},
                             last_error = NULL,
                             updated_at = CURRENT_TIMESTAMP(6)
                         WHERE batch_id = ${result.batchId}
                           AND job_id = ${result.jobId}
                           AND status IN ('pending', 'dispatched', 'running', 'completed')""".update.run
      _ <- if updated == 1 then
        sql"""UPDATE sync_jobs
               SET status = 'completed',
                   finished_at = COALESCE(finished_at, CURRENT_TIMESTAMP(6))
               WHERE job_id = ${result.jobId}
                 AND NOT EXISTS (
                   SELECT 1 FROM sync_batches
                   WHERE job_id = ${result.jobId}
                     AND status <> 'completed'
                 )""".update.run.void *>
          IngestionSql.advanceCursor(streamName, cursorEnd).run.void
      else ().pure[ConnectionIO]
    yield ()

  def scheduleRetry(result: TidbResult, batch: BatchState): ConnectionIO[Unit] =
    sql"""UPDATE sync_batches
           SET status = 'pending',
               last_error = ${result.errorText},
               updated_at = CURRENT_TIMESTAMP(6)
           WHERE batch_id = ${result.batchId}
             AND job_id = ${batch.jobId}
             AND status IN ('dispatched', 'running', 'pending')""".update.run.void

  def completeFailed(result: TidbResult, batch: BatchState): ConnectionIO[Unit] =
    sql"""UPDATE sync_batches
           SET status = 'failed',
               last_error = ${result.errorText},
               updated_at = CURRENT_TIMESTAMP(6)
           WHERE batch_id = ${result.batchId}
             AND job_id = ${batch.jobId}""".update.run.void *>
      sql"""UPDATE sync_jobs
             SET status = 'failed',
                 finished_at = COALESCE(finished_at, CURRENT_TIMESTAMP(6))
             WHERE job_id = ${batch.jobId}
               AND status <> 'completed'""".update.run.void *>
      sql"""INSERT INTO sync_errors (job_id, batch_id, error_class, error_text)
             VALUES (${batch.jobId}, ${result.batchId}, ${result.errorClass}, ${result.errorText})""".update.run.void
