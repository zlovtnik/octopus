package com.sslproxy.coordinator.postgres.sql

import cats.syntax.all.*
import com.sslproxy.coordinator.postgres.{LeaseIdentity, OutboxRecord}
import doobie.ConnectionIO
import doobie.implicits.*

object OutboxSql:
  def claim(
    ownerId: String,
    destinationTopics: List[String],
    leaseSeconds: Int,
    token: String
  ): ConnectionIO[Option[OutboxRecord]] =
    val topics = destinationTopics.map(_.trim).filter(_.nonEmpty).distinct
    if ownerId.isBlank || topics.isEmpty then none[OutboxRecord].pure[ConnectionIO]
    else
      val topicClause = topics.map(value => fr0"$value").intercalate(fr",")
      val safeLeaseSeconds = leaseSeconds.max(1)
      val claimUpdate =
        (fr"""WITH candidate AS (
                SELECT outbox_id
                FROM outbox_events
                WHERE destination_topic IN (""" ++ topicClause ++ fr""" )
                  AND status = 'pending'
                  AND (next_attempt_at <= CURRENT_TIMESTAMP
                       OR (lease_expires_at IS NOT NULL AND lease_expires_at <= CURRENT_TIMESTAMP))
                  AND attempt_count < max_attempts
                ORDER BY created_at, outbox_id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
              )
              UPDATE outbox_events AS claimed
              SET status = 'leased',
                  owner_id = $ownerId,
                  lease_token = $token,
                  fence = fence + 1,
                  attempt_count = attempt_count + 1,
                  lease_expires_at = (CURRENT_TIMESTAMP + ($safeLeaseSeconds) * INTERVAL '1 second'),
                  updated_at = CURRENT_TIMESTAMP
              FROM candidate
              WHERE claimed.outbox_id = candidate.outbox_id""").update.run

      for
        updated <- claimUpdate
        claimed <-
          if updated == 1 then
            sql"""SELECT outbox_id, destination_topic, message_key, CAST(payload AS TEXT),
                        attempt_count, max_attempts, owner_id, lease_token, fence
                 FROM outbox_events
                 WHERE owner_id = $ownerId
                   AND lease_token = $token
                   AND status = 'leased'
                 LIMIT 1"""
              .query[(String, String, String, String, Int, Int, String, String, Long)]
              .unique
              .map { case (id, topic, key, payload, attempts, maxAttempts, owner, leaseToken, fence) =>
                Some(
                  OutboxRecord(id, topic, key, payload, attempts, maxAttempts, LeaseIdentity(owner, leaseToken, fence))
                )
              }
          else none[OutboxRecord].pure[ConnectionIO]
      yield claimed

  def acknowledge(record: OutboxRecord, loadBatchId: Option[String]): ConnectionIO[Boolean] =
    for
      updated <- sql"""UPDATE outbox_events
                        SET status = 'published',
                            published_at = CURRENT_TIMESTAMP,
                            owner_id = NULL,
                            lease_token = NULL,
                            lease_expires_at = NULL,
                            last_error = NULL,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE outbox_id = ${record.outboxId}
                          AND status = 'leased'
                          AND owner_id = ${record.lease.ownerId}
                          AND lease_token = ${record.lease.token}
                          AND fence = ${record.lease.fence}
                          AND lease_expires_at > CURRENT_TIMESTAMP""".update.run
      batchUpdated <-
        if updated == 1 then loadBatchId.fold(true.pure[ConnectionIO])(
          markBatchDispatched(record.outboxId, _, record.messageKey)
        )
        else false.pure[ConnectionIO]
      _ <- if updated == 1 then publishAttempt(record, "published", None) else ().pure[ConnectionIO]
    yield updated == 1 && batchUpdated

  def parkMalformed(record: OutboxRecord, errorText: String): ConnectionIO[Boolean] =
    for
      updated <- sql"""UPDATE outbox_events
                        SET status = 'failed',
                            published_at = CURRENT_TIMESTAMP,
                            owner_id = NULL,
                            lease_token = NULL,
                            lease_expires_at = NULL,
                            last_error = $errorText,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE outbox_id = ${record.outboxId}
                          AND status = 'leased'
                          AND owner_id = ${record.lease.ownerId}
                          AND lease_token = ${record.lease.token}
                          AND fence = ${record.lease.fence}
                          AND lease_expires_at > CURRENT_TIMESTAMP""".update.run
      _ <-
        if updated == 1 then publishAttempt(record, "failed", Some(errorText))
        else ().pure[ConnectionIO]
    yield updated == 1

  def fail(
    record: OutboxRecord,
    status: String,
    errorText: String,
    delaySeconds: Long
  ): ConnectionIO[Unit] =
    for
      updated <- sql"""UPDATE outbox_events
                        SET status = $status,
                            owner_id = NULL,
                            lease_token = NULL,
                            lease_expires_at = NULL,
                            next_attempt_at = (CURRENT_TIMESTAMP + ($delaySeconds) * INTERVAL '1 second'),
                            last_error = $errorText,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE outbox_id = ${record.outboxId}
                          AND status = 'leased'
                          AND owner_id = ${record.lease.ownerId}
                          AND lease_token = ${record.lease.token}
                          AND fence = ${record.lease.fence}
                          AND lease_expires_at > CURRENT_TIMESTAMP""".update.run
      _ <-
        if updated == 1 then publishAttempt(record, status, Some(errorText))
        else doobie.free.connection.raiseError(IllegalStateException(s"lost outbox lease ${record.outboxId}"))
    yield ()

  val RecoverExpiredLeases: ConnectionIO[Int] =
    for
      parked <- sql"""UPDATE outbox_events
                       SET status = 'failed',
                           owner_id = NULL,
                           lease_token = NULL,
                           lease_expires_at = NULL,
                           last_error = 'publish lease expired; max attempts reached',
                           updated_at = CURRENT_TIMESTAMP
                       WHERE status = 'leased'
                         AND lease_expires_at <= CURRENT_TIMESTAMP
                         AND attempt_count >= max_attempts""".update.run
      retried <- sql"""UPDATE outbox_events
                        SET status = 'pending',
                            owner_id = NULL,
                            lease_token = NULL,
                            lease_expires_at = NULL,
                            next_attempt_at = CURRENT_TIMESTAMP,
                            last_error = 'publish lease expired; retrying',
                            updated_at = CURRENT_TIMESTAMP
                        WHERE status = 'leased'
                          AND lease_expires_at <= CURRENT_TIMESTAMP
                          AND attempt_count < max_attempts""".update.run
    yield parked + retried

  private def publishAttempt(
    record: OutboxRecord,
    status: String,
    errorText: Option[String]
  ): ConnectionIO[Unit] =
    sql"""INSERT INTO outbox_publish_attempts (
             outbox_id, attempt_no, status, error_text, attempted_at
           ) VALUES (
             ${record.outboxId}, ${record.attemptCount}, $status, $errorText, CURRENT_TIMESTAMP
           ) ON CONFLICT (outbox_id, attempt_no) DO UPDATE SET
             status = EXCLUDED.status,
             error_text = EXCLUDED.error_text,
             attempted_at = CURRENT_TIMESTAMP""".update.run.void

  private def markBatchDispatched(outboxId: String, batchId: String, messageKey: String): ConnectionIO[Boolean] =
    val prefix = s"$batchId:"
    val expectedAttempt =
      Option.when(messageKey.startsWith(prefix))(messageKey.substring(prefix.length))
        .flatMap(_.toIntOption)
        .filter(_ > 0)
    expectedAttempt match
      case None => doobie.free.connection.raiseError(IllegalArgumentException("load outbox message key is invalid"))
      case Some(attempt) => markBatchDispatched(outboxId, batchId, attempt)

  private def markBatchDispatched(outboxId: String, batchId: String, attempt: Int): ConnectionIO[Boolean] =
    sql"""UPDATE sync_batches
           SET status = 'dispatched',
               attempt_count = $attempt,
               outbox_id = $outboxId,
               last_error = NULL,
               updated_at = CURRENT_TIMESTAMP
           WHERE batch_id = $batchId
             AND status = 'pending'
             AND attempt_count = ${attempt - 1}""".update.run.map(_ == 1)
      .flatTap {
      case true =>
        sql"""UPDATE sync_jobs AS j
               SET status = 'running',
                   started_at = COALESCE(j.started_at, CURRENT_TIMESTAMP)
               FROM sync_batches AS b
               WHERE b.job_id = j.job_id
                 AND b.batch_id = $batchId
                 AND b.status = 'dispatched'
                 AND j.status IN ('pending', 'running')""".update.run.void
      case false => ().pure[ConnectionIO]
    }
