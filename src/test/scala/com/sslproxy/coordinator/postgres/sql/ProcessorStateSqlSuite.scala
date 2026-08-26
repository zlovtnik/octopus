package com.sslproxy.coordinator.postgres.sql

import com.sslproxy.coordinator.processor.{ProcessorId, ProcessorLifecycle, ProcessorStatus}
import munit.FunSuite

import java.time.Instant

class ProcessorStateSqlSuite extends FunSuite:
  test("processor state upsert qualifies retained target columns"):
    val statement = ProcessorStateSql.persistState(
      ProcessorId.SyncScanIngestion,
      ProcessorStatus(ProcessorLifecycle.Starting, 0, None),
      Instant.parse("2026-08-25T00:00:00Z")
    ).sql

    assert(statement.contains(
      "last_started_at = COALESCE(EXCLUDED.last_started_at, processor_state.last_started_at)"
    ))
    assert(statement.contains(
      "last_succeeded_at = COALESCE(EXCLUDED.last_succeeded_at, processor_state.last_succeeded_at)"
    ))
    assert(statement.contains(
      "last_failed_at = COALESCE(EXCLUDED.last_failed_at, processor_state.last_failed_at)"
    ))
