package com.sslproxy.coordinator.postgres.sql

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
    import com.sslproxy.coordinator.processor.ProcessorLifecycle.*
    val databaseStatus = status.lifecycle match
      case Disabled        => "disabled"
      case Starting        => "running"
      case Ready           => "idle"
      case BackingOff      => "degraded"
      case FailedTerminal  => "failed"
    val startedAt = Option.when(status.lifecycle == Starting)(timestamp)
    val succeededAt = Option.when(status.lifecycle == Ready)(timestamp)
    val failedAt = Option.when(
      status.lifecycle == BackingOff || status.lifecycle == FailedTerminal
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
           ) ON CONFLICT (processor_name, shard_id) DO UPDATE SET
             status = EXCLUDED.status,
             last_started_at = COALESCE(EXCLUDED.last_started_at, processor_state.last_started_at),
             last_succeeded_at = COALESCE(EXCLUDED.last_succeeded_at, processor_state.last_succeeded_at),
             last_failed_at = COALESCE(EXCLUDED.last_failed_at, processor_state.last_failed_at),
             consecutive_failures = EXCLUDED.consecutive_failures,
             last_error = EXCLUDED.last_error,
             updated_at = EXCLUDED.updated_at""".update

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

  def runStatus(runId: String): Query0[String] =
    sql"""SELECT status FROM processor_runs WHERE run_id = $runId"""
      .query[String]
