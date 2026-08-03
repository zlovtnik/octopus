package com.sslproxy.coordinator.tidb.sql

import munit.FunSuite

class BatchSinkSqlSuite extends FunSuite:
  test("payload audit replays use the canonical unique key conflict path"):
    val statement = BatchSinkSql.InsertProxyPayloadAudit

    assert(statement.contains("correlation_id, host, direction, captured_at, byte_offset"))
    assert(statement.contains("ON DUPLICATE KEY UPDATE"))
