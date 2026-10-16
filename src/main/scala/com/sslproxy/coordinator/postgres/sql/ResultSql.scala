package com.sslproxy.coordinator.postgres.sql

import cats.syntax.all.*
import com.sslproxy.coordinator.postgres.PostgresResult
import doobie.implicits.*
import doobie.{ConnectionIO, Query0}
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

  def enqueue(result: PostgresResult, attempt: Int, outboxId: String): ConnectionIO[Unit] =
    val safeAttempt = attempt.max(1)
    val messageKey = s"${result.batchId}:$safeAttempt"
    sql"""INSERT INTO outbox_events (
             outbox_id, source_type, source_id, event_type,
             destination_topic, message_key, payload, status,
             attempt_count, max_attempts, next_attempt_at, created_at, updated_at
           ) VALUES (
             $outboxId, 'sync_batch', ${result.batchId}, 'sync.load.result',
             'sync.oracle.result', $messageKey, ${result.asJson.noSpaces}, 'pending',
             0, 5, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
           ) ON CONFLICT ON CONSTRAINT outbox_events_source_event_uq DO UPDATE SET
             message_key = EXCLUDED.message_key,
             payload = EXCLUDED.payload,
             attempt_count = 0,
             max_attempts = EXCLUDED.max_attempts,
             next_attempt_at = CURRENT_TIMESTAMP,
             status = 'pending',
             owner_id = NULL,
             lease_token = NULL,
             lease_expires_at = NULL,
             published_at = NULL,
             last_error = NULL,
             updated_at = CURRENT_TIMESTAMP
           WHERE outbox_events.status IN ('failed', 'cancelled')
              OR (outbox_events.status = 'published'
                  AND outbox_events.message_key <> EXCLUDED.message_key)""".update.run.void

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
    result: PostgresResult,
    streamName: String,
    cursorEnd: String
  ): ConnectionIO[Unit] =
    for
      updated <- sql"""UPDATE sync_batches
                         SET status = 'completed',
                             row_count = ${result.rowCount},
                             checksum = ${result.checksum},
                             last_error = NULL,
                             updated_at = CURRENT_TIMESTAMP
                         WHERE batch_id = ${result.batchId}
                           AND job_id = ${result.jobId}
                           AND status IN ('pending', 'dispatched', 'running')""".update.run
      _ <-
        if updated == 1 then ().pure[ConnectionIO]
        else
          sql"""SELECT status FROM sync_batches WHERE batch_id = ${result.batchId} AND job_id = ${result.jobId}"""
            .query[String]
            .option
            .flatMap {
              case Some("completed") => ().pure[ConnectionIO]
              case _ => requireSingleTransition("complete", result.batchId, updated)
            }
      _ <-
        sql"""UPDATE sync_jobs
               SET status = 'completed',
                   finished_at = COALESCE(finished_at, CURRENT_TIMESTAMP)
               WHERE job_id = ${result.jobId}
                 AND NOT EXISTS (
                   SELECT 1 FROM sync_batches
                   WHERE job_id = ${result.jobId}
                     AND status <> 'completed'
                 )""".update.run.void
      _ <- completeEvents(List(result.jobId))
      _ <- IngestionSql.advanceCursor(streamName, cursorEnd)
    yield ()

  // Load completion does not imply that search projections have consumed the
  // source. This only closes the ingestion ledger; retention keeps its gates.
  private def completeEvents(jobIds: List[String]): ConnectionIO[Int] =
    if jobIds.isEmpty then 0.pure[ConnectionIO]
    else
      val jobs = jobIds.map(id => fr0"$id").intercalate(fr",")
      (fr"""UPDATE sync_events event
             SET status = 'completed', last_error = NULL, updated_at = CURRENT_TIMESTAMP
             FROM sync_jobs job
             WHERE job.job_id IN (""" ++ jobs ++ fr""")
               AND job.status = 'completed'
               AND event.stream_name = job.stream_name
               AND event.dedupe_key = job.dedupe_key
               AND event.status = 'batched'
               AND EXISTS (
                 SELECT 1 FROM sync_batches batch
                 WHERE batch.job_id = job.job_id
                   AND batch.stream_name = job.stream_name
                   AND batch.dedupe_key = job.dedupe_key
                   AND batch.status = 'completed'
               )
               AND NOT EXISTS (
                 SELECT 1 FROM sync_batches batch
                 WHERE batch.job_id = job.job_id AND batch.status <> 'completed'
               )""").update.run

  // One upgrade pass over the job primary key avoids rescanning millions of
  // already repaired rows. The checkpoint lock serializes concurrent callers;
  // its cursor and ledger updates commit together, and resume after a restart.
  def reconcileCompletedEvents(limit: Int): ConnectionIO[Int] =
    val batchSize = limit.max(1).min(1000)
    for
      _ <- sql"""INSERT INTO processor_checkpoints (processor_name, shard_id)
                 VALUES ('sync-backlog-recovery', 'event-completion-v1')
                 ON CONFLICT (processor_name, shard_id) DO NOTHING""".update.run
      checkpoint <- sql"""SELECT checkpoint_value, status FROM processor_checkpoints
                          WHERE processor_name = 'sync-backlog-recovery'
                            AND shard_id = 'event-completion-v1'
                          FOR UPDATE""".query[(Option[String], String)].unique
      repaired <-
        if checkpoint._2 == "healthy" then 0.pure[ConnectionIO]
        else
          for
            ids <- sql"""SELECT job_id FROM sync_jobs
                         WHERE job_id > ${checkpoint._1.getOrElse("")}
                         ORDER BY job_id LIMIT $batchSize""".query[String].to[List]
            changed <- completeEvents(ids)
            _ <- sql"""UPDATE processor_checkpoints
                       SET checkpoint_value = ${ids.lastOption.orElse(checkpoint._1)},
                           status = ${if ids.size < batchSize then "healthy" else "running"},
                           last_started_at = COALESCE(last_started_at, CURRENT_TIMESTAMP),
                           last_succeeded_at = CURRENT_TIMESTAMP,
                           rows_processed = rows_processed + ${ids.size.toLong},
                           updated_at = CURRENT_TIMESTAMP
                       WHERE processor_name = 'sync-backlog-recovery'
                         AND shard_id = 'event-completion-v1'""".update.run
          yield changed
    yield repaired

  def scheduleRetry(result: PostgresResult, batch: BatchState): ConnectionIO[Unit] =
    sql"""UPDATE sync_batches
           SET status = 'pending',
               last_error = ${result.errorText},
               updated_at = CURRENT_TIMESTAMP
           WHERE batch_id = ${result.batchId}
             AND job_id = ${batch.jobId}
             AND status IN ('pending', 'dispatched', 'running')""".update.run.flatMap { updated =>
      requireSingleTransition("schedule retry", result.batchId, updated)
    }

  def completeFailed(result: PostgresResult, batch: BatchState): ConnectionIO[Unit] =
    for
      updated <- sql"""UPDATE sync_batches
           SET status = 'failed',
               last_error = ${result.errorText},
               updated_at = CURRENT_TIMESTAMP
           WHERE batch_id = ${result.batchId}
             AND job_id = ${batch.jobId}
             AND status IN ('pending', 'dispatched', 'running')""".update.run
      _ <- requireSingleTransition("fail", result.batchId, updated)
      _ <- sql"""UPDATE sync_jobs
             SET status = 'failed',
                 finished_at = COALESCE(finished_at, CURRENT_TIMESTAMP)
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
