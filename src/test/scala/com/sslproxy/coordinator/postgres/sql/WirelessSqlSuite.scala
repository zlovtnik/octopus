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
    assert(!probe.contains("CAST("), probe)
    assert(probe.contains("authorized.bssid = ?"), probe)
    assert(probe.contains("authorized.bssid IS NULL"), probe)
    assert(probe.contains("authorized.location_id IS NULL"), probe)

  test("upsert with optional bssid and location_id omits null parameters"):
    val probeWithBssid = WirelessSql
      .upsertClientProbe(
        "network",
        "aa:bb:cc:dd:ee:ff",
        Some("11:22:33:44:55:66"),
        None,
        None,
        1L,
        Some("loc-1"),
        "batch"
      )
      .update
      .sql

    assert(probeWithBssid.contains("authorized.bssid = ?"), probeWithBssid)
    assert(probeWithBssid.contains("authorized.location_id = ?"), probeWithBssid)
    assert(!probeWithBssid.contains("CAST("), probeWithBssid)

    val probeNoOptionals = WirelessSql
      .upsertClientProbe(
        "network",
        "aa:bb:cc:dd:ee:ff",
        None,
        None,
        None,
        1L,
        None,
        "batch"
      )
      .update
      .sql

    assert(probeNoOptionals.contains("authorized.bssid IS NULL"), probeNoOptionals)
    assert(probeNoOptionals.contains("authorized.location_id IS NULL"), probeNoOptionals)
    assert(!probeNoOptionals.contains("CAST("), probeNoOptionals)
