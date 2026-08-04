package com.sslproxy.coordinator.tidb.sql

import munit.FunSuite

class BatchSinkSqlSuite extends FunSuite:
  private val InsertStatement =
    raw"(?is)^\s*INSERT\s+INTO\s+\S+\s*\((.*?)\)\s*VALUES\s*\((.*?)\)".r

  private val insertStatements = List(
    "InsertProxyEvents" -> BatchSinkSql.InsertProxyEvents,
    "UpsertBlockedHostRollups" -> BatchSinkSql.UpsertBlockedHostRollups,
    "InsertProxyPayloadAudit" -> BatchSinkSql.InsertProxyPayloadAudit,
    "InsertWirelessAuditFrames" -> BatchSinkSql.InsertWirelessAuditFrames,
    "UpsertWirelessSensors" -> BatchSinkSql.UpsertWirelessSensors,
    "InsertWirelessBandwidthWindows" -> BatchSinkSql.InsertWirelessBandwidthWindows,
    "UpsertBandwidthAlerts" -> BatchSinkSql.UpsertBandwidthAlerts,
    "UpsertWirelessAlerts" -> BatchSinkSql.UpsertWirelessAlerts,
    "UpsertWirelessClientInventory" -> BatchSinkSql.UpsertWirelessClientInventory,
    "InsertWirelessProbeRequests" -> BatchSinkSql.InsertWirelessProbeRequests
  )

  test("insert statements have one bind placeholder per column"):
    insertStatements.foreach { case (name, statement) =>
      InsertStatement.findFirstMatchIn(statement) match
        case Some(statementMatch) =>
          val columns = statementMatch.group(1)
          val values = statementMatch.group(2)
          val columnCount = columns.split(',').count(_.trim.nonEmpty)
          val bindCount = values.count(_ == '?')

          assertEquals(bindCount, columnCount, name)
        case _ => fail(s"$name is not a recognized INSERT statement")
    }
  test("payload audit replays use the canonical unique key conflict path"):
    val statement = BatchSinkSql.InsertProxyPayloadAudit

    assert(statement.contains("correlation_id, host, direction, captured_at, byte_offset"))
    assert(statement.contains("ON DUPLICATE KEY UPDATE"))

  test("wireless client inventory preserves monotonic observation bounds"):
    val statement = BatchSinkSql.UpsertWirelessClientInventory

    assert(statement.contains("last_seen = GREATEST"))
    assert(statement.contains("first_seen = LEAST"))
