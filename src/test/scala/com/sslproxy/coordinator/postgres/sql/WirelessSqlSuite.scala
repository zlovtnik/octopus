package com.sslproxy.coordinator.postgres.sql

import munit.FunSuite

class WirelessSqlSuite extends FunSuite:
  test("backlog operations bind limits and cutoffs") {
    val listSql = WirelessSql.oldestPending(100).update.sql
    val failSql = WirelessSql.markFailed("dedupe", "stream", "sync_failed", "invalid JSON", 30L).update.sql
    val pruneSql = WirelessSql.pruneSynced(java.sql.Timestamp.from(java.time.Instant.EPOCH), 1000).update.sql
    assert(listSql.contains("LIMIT ?"), listSql)
    assert(failSql.contains("status = ?"), failSql)
    assert(failSql.contains("status IN ('pending', 'sync_failed')"), failSql)
    assert(failSql.contains("next_attempt_at = (CURRENT_TIMESTAMP + (?) * INTERVAL '1 second')"), failSql)
    assert(failSql.contains("attempt_count = attempt_count + 1"), failSql)
    assert(pruneSql.contains("updated_at < ?"), pruneSql)
    assert(pruneSql.contains("LIMIT ?"), pruneSql)
  }

  test("indexed identity comparisons normalize parameters without wrapping columns"):
    val lookup = WirelessSql.lookupDevice("AA:BB:CC:DD:EE:FF").update.sql
    val probe = WirelessSql
      .upsertClientProbe(
        "network",
        "aa:bb:cc:dd:ee:ff",
        Some("11:22:33:44:55:66"),
        None,
        None,
        1L,
        None,
        "batch"
      )
      .update
      .sql

    assert(lookup.contains("WHERE mac_id = ?"), lookup)
    assert(!lookup.contains("LOWER(mac_id)"), lookup)
    assert(!probe.contains("LOWER(authorized.bssid)"), probe)
    assert(probe.contains("CAST(? AS TEXT) IS NOT NULL"), probe)
    assert(probe.contains("authorized.bssid = CAST(? AS TEXT)"), probe)
    assert(probe.contains("authorized.location_id = CAST(? AS TEXT)"), probe)
