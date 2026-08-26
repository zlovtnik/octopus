package com.sslproxy.coordinator.postgres.sql

import munit.FunSuite

class ThreatRiskSqlSuite extends FunSuite:
  test("AP risk supporting aggregates are restricted to bounded candidate BSSIDs"):
    val statement = ThreatRiskSql.apRiskCandidates(25).sql

    assert(statement.contains("JOIN bssids ON bssids.bssid = COALESCE"))
    assert(statement.contains("JOIN bssids ON bssids.bssid = frame.bssid"))
    assert(statement.contains("vendor_counts AS"))
    assert(statement.contains("GROUP BY peer.ssid"))
    assert(statement.contains("UNION ALL"))
    assert(statement.contains("JOIN bssids ON bssids.bssid = attributed.bssid"))
    assert(!statement.contains("GROUP BY COALESCE(left_document.bssid, right_document.bssid)"))

  test("risk persistence matches PostgreSQL boolean and table columns"):
    val implementation = java.nio.file.Files.readString(
      java.nio.file.Paths.get("src/main/scala/com/sslproxy/coordinator/postgres/sql/ThreatRiskSql.scala")
    )
    val apRisk = implementation.substring(implementation.indexOf("def persistApRisk"))
    assert(implementation.contains("${value.sourceKey}, FALSE, FALSE"))
    assert(!apRisk.contains("updated_at"))
