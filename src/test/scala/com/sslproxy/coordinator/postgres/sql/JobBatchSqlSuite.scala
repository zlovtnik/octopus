package com.sslproxy.coordinator.postgres.sql

import munit.FunSuite

class JobBatchSqlSuite extends FunSuite:
  test("load dispatch builder rejects an empty stream set"):
    assertEquals(JobBatchSql.prepareLoadDispatch(Nil, 5, 100), None)

  test("load dispatch binds streams attempts and limits"):
    val statement = JobBatchSql.prepareLoadDispatch(
      List("proxy.events", "wireless.audit", "proxy.events", " "),
      maxAttempts = 5,
      limit = 100
    ).getOrElse(fail("expected a statement"))

    assert(statement.sql.contains("INSERT INTO outbox_events"))
    assert(statement.sql.contains("destination_topic"))
    assert(statement.sql.contains("LIMIT ?"))
    assert(statement.sql.contains("status = IF(status IN"))
    assert(statement.sql.contains("attempt_count = IF(status IN"))
    assert(statement.sql.contains("lease_expires_at = IF(status IN"))
    assert(!statement.sql.contains("proxy.events"))
    assert(!statement.sql.contains("wireless.audit"))
