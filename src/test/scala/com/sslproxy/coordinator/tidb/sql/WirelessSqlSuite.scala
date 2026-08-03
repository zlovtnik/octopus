package com.sslproxy.coordinator.tidb.sql

import munit.FunSuite

class WirelessSqlSuite extends FunSuite:
  test("backlog operations bind limits and cutoffs") {
    val listSql = WirelessSql.oldestPending(100).update.sql
    val failSql = WirelessSql.markFailed("dedupe", "stream", "invalid JSON").update.sql
    val pruneSql = WirelessSql.pruneSynced(java.sql.Timestamp.from(java.time.Instant.EPOCH), 1000).update.sql
    assert(listSql.contains("LIMIT ?"), listSql)
    assert(failSql.contains("status = 'failed'"), failSql)
    assert(failSql.contains("status IN ('pending', 'sync_failed')"), failSql)
    assert(pruneSql.contains("updated_at < ?"), pruneSql)
    assert(pruneSql.contains("LIMIT ?"), pruneSql)
  }
