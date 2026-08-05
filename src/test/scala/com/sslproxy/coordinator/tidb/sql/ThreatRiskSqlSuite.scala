package com.sslproxy.coordinator.tidb.sql

import munit.FunSuite

class ThreatRiskSqlSuite extends FunSuite:
  test("AP risk supporting aggregates are restricted to bounded candidate BSSIDs"):
    val statement = ThreatRiskSql.apRiskCandidates(25).sql

    assert(statement.contains("JOIN bssids ON bssids.bssid = COALESCE"))
    assert(statement.contains("JOIN bssids ON bssids.bssid = frame.bssid"))
    assert(statement.contains("bssid_left.bssid = left_document.bssid"))
    assert(statement.contains("bssid_right.bssid = right_document.bssid"))
