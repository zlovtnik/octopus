package com.sslproxy.coordinator.postgres.sql

import munit.FunSuite

import java.nio.file.{Files, Paths}

class ResultSqlSuite extends FunSuite:
  private lazy val implementation: String =
    val source = Files.readString(
      Paths.get("src/main/scala/com/sslproxy/coordinator/postgres/sql/ResultSql.scala")
    )
    // Reconciliation has its own conflict handling; these checks cover enqueue only.
    source.substring(source.indexOf("  def enqueue("), source.indexOf("  def batchForUpdate("))

  test("result enqueue reopens only terminally failed messages"):
    assert(implementation.contains("ON CONFLICT DO NOTHING"))
    assert(implementation.contains("UPDATE outbox_events SET"))
    assert(implementation.contains("AND message_key = $messageKey"))
    assert(implementation.contains("AND status IN ('failed', 'cancelled')"))
    assert(implementation.contains("sync_batch_result"))
    assert(implementation.contains("status = 'pending'"))
    assert(implementation.contains("published_at = NULL"))

  test("result enqueue never reopens an in-flight or published message"):
    assert(!implementation.contains("status = CASE WHEN outbox_events.status IN"))
    assert(!implementation.contains("owner_id = CASE WHEN"))
