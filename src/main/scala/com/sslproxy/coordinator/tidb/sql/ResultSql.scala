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
           ) ON DUPLICATE KEY UPDATE
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
             updated_at = CURRENT_TIMESTAMP(6)""".update.run.void

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
                           AND status IN ('pending', 'dispatched', 'running')""".update.run
      _ <- if updated == 1 then ().pure[ConnectionIO]
            else
              sql"""SELECT status FROM sync_batches WHERE batch_id = ${result.batchId} AND job_id = ${result.jobId}""".query[String].option.flatMap {
                case Some("completed") => ().pure[ConnectionIO]
                case _ => requireSingleTransition("complete", result.batchId, updated)
              }
      _ <-
        sql"""UPDATE sync_jobs
               SET status = 'completed',
                   finished_at = COALESCE(finished_at, CURRENT_TIMESTAMP(6))
               WHERE job_id = ${result.jobId}
                 AND NOT EXISTS (
                   SELECT 1 FROM sync_batches
                   WHERE job_id = ${result.jobId}
                     AND status <> 'completed'
                 )""".update.run.void
      _ <- IngestionSql.advanceCursor(streamName, cursorEnd)
    yield ()

  def scheduleRetry(result: TidbResult, batch: BatchState): ConnectionIO[Unit] =
    sql"""UPDATE sync_batches
           SET status = 'pending',
               last_error = ${result.errorText},
               updated_at = CURRENT_TIMESTAMP(6)
           WHERE batch_id = ${result.batchId}
             AND job_id = ${batch.jobId}
             AND status IN ('pending', 'dispatched', 'running')""".update.run.flatMap { updated =>
      requireSingleTransition("schedule retry", result.batchId, updated)
    }

  def completeFailed(result: TidbResult, batch: BatchState): ConnectionIO[Unit] =
    for
      updated <- sql"""UPDATE sync_batches
           SET status = 'failed',
               last_error = ${result.errorText},
               updated_at = CURRENT_TIMESTAMP(6)
           WHERE batch_id = ${result.batchId}
             AND job_id = ${batch.jobId}
             AND status IN ('pending', 'dispatched', 'running')""".update.run
      _ <- requireSingleTransition("fail", result.batchId, updated)
      _ <- sql"""UPDATE sync_jobs
             SET status = 'failed',
                 finished_at = COALESCE(finished_at, CURRENT_TIMESTAMP(6))
             WHERE job_id = ${batch.jobId}
               AND status <> 'completed'""".update.run.void
      _ <- sql"""INSERT INTO sync_errors (job_id, batch_id, error_class, error_text)
             VALUES (${batch.jobId}, ${result.batchId}, ${result.errorClass}, ${result.errorText})""".update.run.void
    yield ()

  private def requireSingleTransition(
    transition: String,
    batchId: String,
    affectedRows: Int
  ): ConnectionIO[Unit] =
    if affectedRows == 1 then ().pure[ConnectionIO]
    else
      doobie.free.connection.raiseError(
        IllegalStateException(
          s"cannot $transition batch $batchId from its current state; affected rows: $affectedRows"
        )
      )
