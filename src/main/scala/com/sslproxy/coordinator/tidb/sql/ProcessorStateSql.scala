package com.sslproxy.coordinator.tidb.sql

import com.sslproxy.coordinator.processor.{ProcessorId, ProcessorRunStatus, ProcessorStatus}
import doobie.{Query0, Update0}
import doobie.implicits.*

import java.sql.Timestamp
import java.time.Instant

object ProcessorStateSql:
  val LoadStatesQuery: Query0[(String, String, Int, Option[String])] =
    sql"""SELECT processor_name, status,
                  consecutive_failures, last_error
           FROM processor_state"""
      .query[(String, String, Int, Option[String])]

  def persistState(
      id: ProcessorId,
      status: ProcessorStatus,
      observedAt: Instant
  ): Update0 =
    val timestamp = Timestamp.from(observedAt)
    val databaseStatus = status.lifecycle.value match
      case "disabled"        => "disabled"
      case "starting"        => "running"
      case "ready"           => "idle"
      case "backing_off"     => "degraded"
      case "failed_terminal" => "failed"
    val startedAt = Option.when(status.lifecycle.value == "starting")(timestamp)
    val succeededAt = Option.when(status.lifecycle.value == "ready")(timestamp)
    val failedAt = Option.when(
      status.lifecycle.value == "backing_off" || status.lifecycle.value == "failed_terminal"
    )(timestamp)
    val failures = if failedAt.nonEmpty then status.restartCount.max(1) else 0

    sql"""INSERT INTO processor_state (
             processor_name, shard_id, status,
             last_started_at, last_succeeded_at, last_failed_at,
             consecutive_failures, last_error, updated_at
           ) VALUES (
             ${id.value}, 'default', $databaseStatus,
             $startedAt, $succeededAt, $failedAt,
             $failures, ${status.lastError}, $timestamp
           ) ON DUPLICATE KEY UPDATE
             status = VALUES(status),
             last_started_at = COALESCE(VALUES(last_started_at), last_started_at),
             last_succeeded_at = COALESCE(VALUES(last_succeeded_at), last_succeeded_at),
             last_failed_at = COALESCE(VALUES(last_failed_at), last_failed_at),
             consecutive_failures = VALUES(consecutive_failures),
             last_error = VALUES(last_error),
             updated_at = VALUES(updated_at)""".update

  def startRun(id: ProcessorId, runId: String, startedAt: Instant): Update0 =
    sql"""INSERT INTO processor_runs (
             run_id, processor_name, shard_id, status, started_at
           ) VALUES (
             $runId, ${id.value}, 'default', ${ProcessorRunStatus.Running.value}, ${Timestamp.from(startedAt)}
           )""".update

  def finishRun(
      runId: String,
      status: ProcessorRunStatus,
      errorClass: Option[String],
      errorText: Option[String],
      finishedAt: Instant
  ): Update0 =
    sql"""UPDATE processor_runs
           SET status = ${status.value},
               finished_at = ${Timestamp.from(finishedAt)},
               error_class = $errorClass,
               error_text = $errorText
           WHERE run_id = $runId AND status = ${ProcessorRunStatus.Running.value}""".update
