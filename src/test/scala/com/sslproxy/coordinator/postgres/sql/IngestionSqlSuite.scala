package com.sslproxy.coordinator.postgres.sql

import munit.FunSuite

class IngestionSqlSuite extends FunSuite:
  test("hydration candidates exclude quarantined rows"):
    val statement = IngestionSql.hydrationCandidates(None, 10).sql

    assert(statement.contains("e.status <> 'failed'"))

  test("hydration quarantine records a terminal event failure"):
    val statement = IngestionSql.quarantineHydration("proxy.events", "dedupe", "missing payload").sql

    assert(statement.contains("status = 'failed'"))
    assert(statement.contains("attempt_count = attempt_count + 1"))
    assert(statement.contains("last_error = ?"))
