package com.sslproxy.coordinator.postgres.sql

import munit.FunSuite

class BatchSinkSqlSuite extends FunSuite:
  private def topLevelParts(s: String): List[String] =
    var depth = 0
    val current = new StringBuilder
    val parts = scala.collection.mutable.ListBuffer.empty[String]
    for ch <- s do
      ch match
        case '(' => depth += 1; current.append(ch)
        case ')' => depth -= 1; current.append(ch)
        case ',' if depth == 0 =>
          parts += current.toString.trim
          current.clear()
        case _ => current.append(ch)
    if current.nonEmpty then parts += current.toString.trim
    parts.toList

  private def extractGroup(sql: String, prefix: String): Option[String] =
    val upperSql = sql.toUpperCase
    val prefixIdx = upperSql.indexOf(prefix.toUpperCase)
    if prefixIdx < 0 then return None
    val parenStart = sql.indexOf('(', prefixIdx + prefix.length)
    if parenStart < 0 then return None
    var depth = 1
    var i = parenStart + 1
    while i < sql.length && depth > 0 do
      sql.charAt(i) match
        case '(' => depth += 1
        case ')' => depth -= 1
        case _ =>
      i += 1
    if depth == 0 then Some(sql.substring(parenStart + 1, i - 1))
    else None

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
      val columnsOpt = extractGroup(statement, "INSERT INTO")
      val valuesOpt = extractGroup(statement, "VALUES")
      (columnsOpt, valuesOpt) match
        case (Some(columns), Some(values)) =>
          val columnCount = topLevelParts(columns).count(_.nonEmpty)
          val bindCount = values.count(_ == '?')
          assertEquals(bindCount, columnCount, name)
        case _ => fail(s"$name is not a recognized INSERT statement")
    }
  test("payload audit replays use the canonical unique key conflict path"):
    val statement = BatchSinkSql.InsertProxyPayloadAudit

    assert(statement.contains("correlation_id, host, direction, captured_at, byte_offset"))
    assert(statement.contains("ON CONFLICT (correlation_id, direction, byte_offset) DO UPDATE SET"))

  test("wireless client inventory preserves monotonic observation bounds"):
    val statement = BatchSinkSql.UpsertWirelessClientInventory

    assert(statement.contains("last_seen = GREATEST"))
    assert(statement.contains("first_seen = LEAST"))

  test("bandwidth alert byte accumulation treats null operands as zero"):
    val statement = BatchSinkSql.UpsertBandwidthAlerts

    assert(
      statement.contains(
        "bytes = COALESCE(wireless_alerts.bytes, 0) + COALESCE(EXCLUDED.bytes, 0)"
      )
    )
