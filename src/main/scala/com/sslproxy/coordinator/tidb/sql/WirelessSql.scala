package com.sslproxy.coordinator.tidb.sql

import doobie.Fragment
import doobie.implicits.*
import io.circe.Json

import java.sql.Timestamp

object WirelessSql:
  def upsertBacklog(
      dedupeKey: String,
      streamName: String,
      payload: Json,
      failureStage: String
  ): Fragment =
    sql"""INSERT INTO sync_backlog (
            dedupe_key, stream_name, payload, failure_stage, status,
            attempt_count, next_attempt_at, created_at, updated_at
          ) VALUES (
            $dedupeKey, $streamName, ${payload.noSpaces}, $failureStage, 'pending',
            0, CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6)
          ) ON DUPLICATE KEY UPDATE
            payload = VALUES(payload),
            failure_stage = VALUES(failure_stage),
            status = CASE WHEN sync_backlog.status = 'synced' THEN 'synced' ELSE 'pending' END,
            updated_at = CURRENT_TIMESTAMP(6)"""

  def oldestPending(limit: Int): Fragment =
    sql"""SELECT dedupe_key, stream_name, CAST(payload AS CHAR), failure_stage,
                 attempt_count, created_at
          FROM sync_backlog
          WHERE status IN ('pending', 'sync_failed')
            AND next_attempt_at <= CURRENT_TIMESTAMP(6)
          ORDER BY created_at, stream_name, dedupe_key
          LIMIT $limit"""

  def markSynced(dedupeKey: String, streamName: String): Fragment =
    sql"""UPDATE sync_backlog
          SET status = 'synced', last_error = NULL, updated_at = CURRENT_TIMESTAMP(6)
          WHERE dedupe_key = $dedupeKey AND stream_name = $streamName
            AND status <> 'synced'"""

  def pruneSynced(cutoff: Timestamp, limit: Int): Fragment =
    sql"""DELETE FROM sync_backlog
          WHERE status = 'synced' AND updated_at < $cutoff
          ORDER BY updated_at, stream_name, dedupe_key
          LIMIT $limit"""
