package com.sslproxy.coordinator.postgres.sql

import munit.FunSuite

import java.nio.file.{Files, Paths}

class ResultSqlSuite extends FunSuite:
  private lazy val implementation: String = Files.readString(
    Paths.get("src/main/scala/com/sslproxy/coordinator/postgres/sql/ResultSql.scala")
  )

  test("result enqueue reopens only terminally failed messages"):
    assert(
      implementation.contains(
        "ON CONFLICT ON CONSTRAINT outbox_events_source_event_uq DO UPDATE SET"
      )
    )
    assert(implementation.contains("message_key = EXCLUDED.message_key"))
    assert(implementation.contains("WHERE outbox_events.status IN ('failed', 'cancelled')"))
    assert(implementation.contains("outbox_events.message_key <> EXCLUDED.message_key"))
    assert(implementation.contains("status = 'pending'"))
    assert(implementation.contains("published_at = NULL"))
    assert(!implementation.contains("DO NOTHING"))

  test("result enqueue never reopens an in-flight or published message"):
    assert(!implementation.contains("status = CASE WHEN outbox_events.status IN"))
    assert(!implementation.contains("owner_id = CASE WHEN"))
