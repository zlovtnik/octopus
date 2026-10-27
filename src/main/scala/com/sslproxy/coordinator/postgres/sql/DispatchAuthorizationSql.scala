package com.sslproxy.coordinator.postgres.sql

import cats.syntax.all.*
import com.sslproxy.coordinator.postgres.{PostgresLoad, PostgresResult}
import doobie.ConnectionIO
import doobie.implicits.*
import io.circe.parser.decode

/** Broker reachability is not authority. Only coordinator durable outbox
  * records authorize effects.
  */
object DispatchAuthorizationSql:
  def authorizeLoad(load: PostgresLoad): ConnectionIO[Unit] =
    sql"""SELECT o.payload::text
           FROM outbox_events o JOIN sync_batches b ON b.batch_id = split_part(o.source_id, ':', 1)::uuid
           WHERE o.source_type IN ('sync_batch', 'sync_batch_retry')
             AND split_part(o.source_id, ':', 1) = ${load.batchId}
             AND o.event_type = 'sync.load.requested' AND o.destination_topic = 'sync.oracle.load'
             AND o.message_key = ${s"${load.batchId}:${load.attempt}"}
             AND o.status IN ('pending', 'leased', 'published')
             AND b.job_id = ${load.jobId} AND b.stream_name = ${load.streamName}
             AND b.payload_ref = ${load.payloadRef}
             AND b.cursor_start = ${load.cursorStart} AND b.cursor_end = ${load.cursorEnd}"""
      .query[String]
      .option
      .flatMap { stored =>
        requireAuthorized(
          stored
            .exists(payload => decode[PostgresLoad](payload).contains(load)),
          "load"
        )
      }

  def authorizeResult(
    result: PostgresResult,
    messageKey: Option[String]
  ): ConnectionIO[Unit] =
    sql"""SELECT o.payload::text
           FROM outbox_events o JOIN sync_batches b ON b.batch_id = o.source_id
           JOIN outbox_events dispatch ON split_part(dispatch.source_id, ':', 1) = b.batch_id::text
             AND dispatch.source_type IN ('sync_batch', 'sync_batch_retry')
             AND dispatch.event_type = 'sync.load.requested'
             AND dispatch.destination_topic = 'sync.oracle.load' AND dispatch.message_key = o.message_key
           WHERE o.source_type = 'sync_batch' AND o.source_id = ${result.batchId}
             AND o.event_type = 'sync.load.result' AND o.destination_topic = 'sync.oracle.result'
             AND o.message_key = ${messageKey.getOrElse("")}
             AND b.job_id = ${result.jobId}
             AND o.status IN ('pending', 'leased', 'published')"""
      .query[String]
      .option
      .flatMap { stored =>
        requireAuthorized(
          stored.exists(payload => decode[PostgresResult](payload).contains(result)),
          "result"
        )
      }

  private def requireAuthorized(
    allowed: Boolean,
    kind: String
  ): ConnectionIO[Unit] =
    if allowed then ().pure[ConnectionIO]
    else
      doobie.free.connection.raiseError(
        IllegalArgumentException(s"unauthorized coordinator $kind")
      )
