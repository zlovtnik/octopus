package com.sslproxy.coordinator.tidb

import cats.effect.{IO, Resource}
import com.sslproxy.coordinator.config.TiDbConfig
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import io.circe.{Json, parser as circeParser}
import com.sslproxy.coordinator.observability.StructuredLogger
import com.sslproxy.coordinator.tidb.sql.{BatchSinkSql, SchemaChecksSql}

import java.sql.{BatchUpdateException, Connection, PreparedStatement, SQLException, Timestamp, Types}
import java.time.{Instant, OffsetDateTime, ZoneOffset}
import scala.concurrent.duration.*

final class TidbTransactor private (
    ds: HikariDataSource,
    config: TiDbConfig,
    tlsMaterial: Option[TidbTlsMaterial]
) extends TidbSink:
  import TidbTransactor.{WirelessAlertRow, log}

  def dataSource: HikariDataSource = ds

  private val batchSize: Int = 500

  private val retryMaxAttempts: Int = 3
  private val retryBaseDelay: FiniteDuration = 200.millis
  private val networkTimeoutExecutor: java.util.concurrent.Executor = command => command.run()

  private def withConnection[A](f: Connection => A): IO[A] =
    IO.blocking {
      val conn = ds.getConnection
      try f(conn)
      finally conn.close()
    }

  private def withTransaction[A](f: Connection => A): IO[A] =
    IO.blocking {
      val conn = ds.getConnection
      try
        conn.setAutoCommit(false)
        if config.statementTimeoutSecs > 0 then
          conn.setNetworkTimeout(networkTimeoutExecutor, config.statementTimeoutSecs * 1000)
        val result = f(conn)
        conn.commit()
        result
      catch
        case e: Exception =>
          rollbackQuietly(conn)
          throw e
      finally conn.close()
    }

  private def withRetry[A](label: String)(f: IO[A]): IO[A] =
    def go(attempt: Int): IO[A] =
      f.handleErrorWith { err =>
        if attempt < retryMaxAttempts && TidbErrorClass.classify(err) == TidbErrorClass.Retryable then
          val delay = retryBaseDelay * (1L << (attempt - 1))
          log.warn("tidb_retry", "status" -> "retrying",
            "operation" -> label, "attempt" -> s"$attempt/$retryMaxAttempts",
            "delay" -> s"${delay.toMillis}ms",
            "error" -> Option(err.getMessage).getOrElse(err.getClass.getSimpleName))
          IO.sleep(delay) *> go(attempt + 1)
        else
          IO.raiseError(err)
      }
    go(1)

  private def withTransactionRetry[A](label: String)(f: Connection => A): IO[A] =
    withRetry(label)(withTransaction(f))

  private def checkConnection(): IO[Unit] =
    withConnection { conn =>
      val stmt = conn.createStatement()
      try
        stmt.setQueryTimeout(5)
        val rs = stmt.executeQuery(BatchSinkSql.ConnectivityQuery)
        rs.next()
        ()
      finally stmt.close()
    }

  def healthCheck: IO[Boolean] =
    checkConnection().as(true).handleError(_ => false)

  private def executeBatch(stmt: PreparedStatement, rows: Seq[Seq[Any]]): Long =
    var count = 0L
    for row <- rows do
      for (value, idx) <- row.zipWithIndex do
        setParam(stmt, idx + 1, value)
      stmt.addBatch()
      count += 1
      if count % batchSize == 0 then
        TidbTransactor.validateBatchResults(stmt.executeBatch())
    val remainder = (count % batchSize).toInt
    if remainder != 0 then
      TidbTransactor.validateBatchResults(stmt.executeBatch())
    rows.size.toLong

  private def setParam(stmt: PreparedStatement, idx: Int, value: Any): Unit =
    value match
      case null              => stmt.setNull(idx, Types.NULL)
      case v: String         => stmt.setString(idx, v)
      case v: Int            => stmt.setInt(idx, v)
      case v: Long           => stmt.setLong(idx, v)
      case v: Boolean        => stmt.setBoolean(idx, v)
      case v: Double         => stmt.setDouble(idx, v)
      case v: Timestamp      => stmt.setTimestamp(idx, v)
      case v: java.sql.Date  => stmt.setDate(idx, v)
      case Some(inner)       => setParam(stmt, idx, inner)
      case None              => stmt.setNull(idx, Types.NULL)
      case v: OffsetDateTime => stmt.setObject(idx, v)
      case v                 => stmt.setString(idx, v.toString)

  private def ts(odt: OffsetDateTime): Timestamp =
    Timestamp.from(odt.toInstant)

  private def optLong(v: Option[Long]): java.lang.Long = v.map(java.lang.Long.valueOf).orNull
  private def optStr(v: Option[String]): String = v.orNull
  private def optDbl(v: Option[Double]): java.lang.Double = v.map(java.lang.Double.valueOf).orNull

  // ── proxy_events ──────────────────────────────────────────────
  override def insertProxyEvents(
      batchId: String,
      rows: List[ProxyEventInsert],
      blockedRows: List[BlockedEventInsert]
  ): IO[Long] =
    withTransactionRetry("insert_proxy_events") { conn =>
      val stmt = conn.prepareStatement(BatchSinkSql.InsertProxyEvents)
      try
        val allRows = rows.zipWithIndex.map { case (r, idx) =>
          Seq[Any](
            batchId, idx + 1L,
            ts(r.eventTime), ts(r.eventTime),
            r.eventType, r.host,
            optStr(r.peerIp), optStr(r.wgPubkey),
            optStr(r.deviceId), r.identitySource,
            optStr(r.peerHostname), optStr(r.clientUa),
            r.bytesUp, r.bytesDown,
            optLong(r.statusCode), r.blocked,
            optStr(r.obfuscationProfile),
            optStr(r.correlationId), optLong(r.parentEventId.map(_.toLong)),
            optLong(r.eventSequence), optLong(r.durationMs),
            optStr(r.reason), optStr(r.rawJson)
          )
        }
        val count = executeBatch(stmt, allRows)
        doInsertBlockedHostRollups(conn, blockedRows)
        count
      finally stmt.close()
    }

  // ── proxy_blocked_host_rollups ────────────────────────────────
  private def doInsertBlockedHostRollups(conn: Connection, rows: List[BlockedEventInsert]): Long =
    if rows.isEmpty then 0L
    else
      val stmt = conn.prepareStatement(BatchSinkSql.UpsertBlockedHostRollups)
      try
        val now = Timestamp.from(Instant.now())
        val params = rows.map(r =>
          Seq[Any](
            r.host, 1L, r.blockedBytes,
            optDbl(r.frequencyHz), r.verdict, r.category,
            optDbl(r.riskScore), r.tarpitHeldMs,
            optLong(r.iatMs), optLong(r.consecutiveBlocks),
            r.lastVerdict, optStr(r.tlsVer), optStr(r.alpn),
            optStr(r.ja3Lite), optStr(r.resolvedIp), optStr(r.asnOrg),
            now, now
          )
        )
        executeBatch(stmt, params)
      finally stmt.close()

  // ── proxy_payload_audit ───────────────────────────────────────
  override def insertProxyPayloadAudit(batchId: String, rows: List[ProxyPayloadAuditInsert]): IO[Long] =
    withTransactionRetry("insert_proxy_payload_audit") { conn =>
      val stmt = conn.prepareStatement(BatchSinkSql.InsertProxyPayloadAudit)
      try
        val params = rows.map(r =>
          Seq[Any](
            r.correlationId, r.host, r.direction, ts(r.capturedAt), r.byteOffset,
            optStr(r.payloadObjectKey), optStr(r.contentType),
            optStr(r.httpMethod), optLong(r.httpStatus), optStr(r.httpPath),
            r.isEncrypted, r.truncated,
            optStr(r.peerIp), optStr(r.notes)
          )
        )
        executeBatch(stmt, params)
      finally stmt.close()
    }

  // ── wireless_audit_frames + sensor upsert ─────────────────────
  override def insertWirelessAuditFrames(batchId: String, rows: List[WirelessAuditFrameInsert]): IO[Long] =
    if rows.isEmpty then IO.pure(0L)
    else withTransactionRetry("insert_wireless_audit_frames") { conn =>

      upsertWirelessSensors(conn, rows)

      val stmt = conn.prepareStatement(BatchSinkSql.InsertWirelessAuditFrames)
      try
        val params = rows.map(r =>
          Seq[Any](
            batchId, r.rowSequence, r.eventType, ts(r.observedAt), r.sensorId, r.locationId,
            r.iface, r.channel, bandForChannel(r.channel),
            optStr(r.frameType), r.frameSubtype,
            optStr(r.bssid), optStr(r.sourceMac), optStr(r.destinationMac),
            optStr(r.transmitterMac), optStr(r.receiverMac), optStr(r.destinationBssid),
            optStr(r.ssid), optLong(r.signalDbm), optLong(r.sequenceNumber),
            r.rawLen, r.isRetry, r.isMoreData, r.isPowerSave,
            r.isProtected, r.isToDs, r.isFromDs, r.isHandshake,
            r.securityFlags, optStr(r.deviceId), optStr(r.username),
            r.identitySource, optStr(r.tags), optStr(r.anomalyReasons),
            optStr(r.rawJson)
          )
        )
        executeBatch(stmt, params)
      finally stmt.close()
    }

  private def upsertWirelessSensors(conn: Connection, rows: List[WirelessAuditFrameInsert]): Unit =
    val sensors = rows.foldLeft(Map.empty[String, WirelessAuditFrameInsert]) { (acc, row) =>
      acc.updated(row.sensorId,
        acc.get(row.sensorId) match
          case Some(existing) if existing.observedAt.isBefore(row.observedAt) => existing
          case _ => row
      )
    }

    val stmt = conn.prepareStatement(BatchSinkSql.UpsertWirelessSensors)
    try
      for (_, row) <- sensors do
        setParam(stmt, 1, row.sensorId)
        setParam(stmt, 2, row.locationId)
        setParam(stmt, 3, row.iface)
        setParam(stmt, 4, optStr(row.regDomain))
        setParam(stmt, 5, ts(row.observedAt))
        setParam(stmt, 6, ts(row.observedAt))
        stmt.addBatch()
      stmt.executeBatch(): Unit
    finally stmt.close()

  // ── wireless_bandwidth_windows + alert merge ──────────────────
  override def insertWirelessBandwidth(batchId: String, rows: List[WirelessBandwidthInsert]): IO[Long] =
    withTransactionRetry("insert_wireless_bandwidth") { conn =>
      val stmt = conn.prepareStatement(BatchSinkSql.InsertWirelessBandwidthWindows)
      try
        val params = rows.map(r =>
          Seq[Any](
            batchId, r.rowSequence, r.schemaVersion,
            ts(r.windowStart), ts(r.windowEnd),
            r.sensorId, r.locationId, r.iface, r.channel, bandForChannel(r.channel),
            r.sourceMac, r.destinationBssid,
            optStr(r.ssid), r.bytes, r.frameCount, r.retryCount,
            r.moreDataCount, r.powerSaveCount,
            optLong(r.strongestSignalDbm),
            r.histUnder100, r.hist100500, r.hist5001000, r.hist10001500,
            optLong(r.interArrivalP50Ms), r.externalBssid, r.thresholdExceeded,
            optLong(r.wallClockDeltaMs), r.windowIsPartial,
            r.publishedAt.map(ts).orNull
          )
        )
        val inserted = executeBatch(stmt, params)
        mergeBandwidthAlerts(conn, batchId, rows)
        inserted
      finally stmt.close()
    }

  private def mergeBandwidthAlerts(conn: Connection, batchId: String, rows: List[WirelessBandwidthInsert]): Long =
    val exceeded = rows.filter(_.thresholdExceeded != 0L)
    if exceeded.isEmpty then return 0L

    val grouped = exceeded.groupBy { r =>
      (r.sensorId, r.sourceMac, r.destinationBssid, r.windowStart.toLocalDate)
    }

    val stmt = conn.prepareStatement(BatchSinkSql.UpsertBandwidthAlerts)
    try
      var alertCount = 0L
      val now = Timestamp.from(Instant.now())
      for (_, group) <- grouped do
        val minRowSeq = group.map(_.rowSequence).min
        val firstWindowStart = group.map(_.windowStart).minBy(_.toInstant)
        val lastByTime = group.maxBy(_.windowStart.toInstant)
        val totalBytes = group.map(_.bytes).sum

        val details = Json.obj(
          "aggregated_rows" -> Json.fromLong(group.length.toLong),
          "total_bytes" -> Json.fromLong(totalBytes),
          "threshold" -> Json.fromString("exceeded")
        ).noSpaces

        alertCount += 1
        setParam(stmt, 1, "bandwidth_threshold")
        setParam(stmt, 2, batchId)
        setParam(stmt, 3, minRowSeq)
        setParam(stmt, 4, java.sql.Date.valueOf(firstWindowStart.toLocalDate))
        setParam(stmt, 5, ts(firstWindowStart))
        setParam(stmt, 6, lastByTime.sensorId)
        setParam(stmt, 7, lastByTime.locationId)
        setParam(stmt, 8, lastByTime.sourceMac)
        setParam(stmt, 9, lastByTime.destinationBssid)
        setParam(stmt, 10, optStr(lastByTime.ssid))
        setParam(stmt, 11, null)
        setParam(stmt, 12, details)
        setParam(stmt, 13, null)
        setParam(stmt, 14, now)
        setParam(stmt, 15, now)
        setParam(stmt, 16, totalBytes)
        stmt.addBatch()
      stmt.executeBatch(): Unit
      alertCount
    finally stmt.close()

  // ── wireless alerts (4 alert types) ────────────────────────────

  override def insertWirelessRogueAp(batchId: String, rows: List[WirelessRogueApInsert]): IO[Long] =
    mergeWirelessAlerts(batchId, "rogue_ap", rows.map { row =>
        WirelessAlertRow(
          rowSequence = row.rowSequence,
          detectedAt = row.detectedAt,
          sensorId = row.sensorId,
          locationId = row.locationId,
          iface = Some(row.iface),
          channel = Some(row.channel),
          primaryMac = Some(row.rogueBssid),
          secondaryMac = None,
          ssid = row.ssid,
          signalDbm = row.signalDbm,
          detailsJson = TidbTransactor.jsonDetails("ssid_impersonation" -> row.ssidImpersonation),
          rawJson = row.rawJson
        )
    })

  override def insertWirelessDeauthFlood(batchId: String, rows: List[WirelessDeauthFloodInsert]): IO[Long] =
    mergeWirelessAlerts(batchId, "deauth_flood", rows.map { row =>
        WirelessAlertRow(
          rowSequence = row.rowSequence,
          detectedAt = row.detectedAt,
          sensorId = row.sensorId,
          locationId = row.locationId,
          iface = Some(row.iface),
          channel = Some(row.channel),
          primaryMac = row.attackerMac,
          secondaryMac = row.targetBssid,
          ssid = row.targetSsid,
          signalDbm = row.signalDbm,
          detailsJson = TidbTransactor.jsonDetails(
            "deauth_count" -> row.deauthCount,
            "window_secs" -> row.windowSecs,
            "threshold" -> row.threshold
          ),
          rawJson = row.rawJson
        )
    })

  override def insertWirelessSignalAnomaly(batchId: String, rows: List[WirelessSignalAnomalyInsert]): IO[Long] =
    mergeWirelessAlerts(batchId, "signal_anomaly", rows.map { row =>
        WirelessAlertRow(
          rowSequence = row.rowSequence,
          detectedAt = row.detectedAt,
          sensorId = row.sensorId,
          locationId = row.locationId,
          iface = None,
          channel = Some(row.channel),
          primaryMac = Some(row.sourceMac),
          secondaryMac = row.bssid,
          ssid = row.ssid,
          signalDbm = Some(row.observedDbm),
          detailsJson = TidbTransactor.jsonDetails(
            "baseline_dbm" -> row.baselineDbm,
            "observed_dbm" -> row.observedDbm,
            "dbm_delta" -> row.dbmDelta,
            "configured_delta" -> row.configuredDelta
          ),
          rawJson = None
        )
    })

  override def insertWirelessPmfAttack(batchId: String, rows: List[WirelessPmfAttackInsert]): IO[Long] =
    mergeWirelessAlerts(batchId, "pmf_attack", rows.map { row =>
        WirelessAlertRow(
          rowSequence = row.rowSequence,
          detectedAt = row.detectedAt,
          sensorId = row.sensorId,
          locationId = row.locationId,
          iface = None,
          channel = row.channel,
          primaryMac = Some(row.targetMac),
          secondaryMac = row.targetBssid,
          ssid = row.ssid,
          signalDbm = None,
          detailsJson = TidbTransactor.jsonDetails(
            "attack_tag" -> row.attackTag,
            "reconnect_window_ms" -> row.reconnectWindowMs
          ),
          rawJson = None
        )
    })

  override def insertWirelessAttackSequence(batchId: String, rows: List[WirelessAttackSequenceInsert]): IO[Long] =
    mergeWirelessAlerts(batchId, "attack_sequence", rows.map { row =>
        WirelessAlertRow(
          rowSequence = row.rowSequence,
          detectedAt = row.detectedAt,
          sensorId = row.sensorId,
          locationId = row.locationId,
          iface = None,
          channel = None,
          primaryMac = None,
          secondaryMac = None,
          ssid = row.ssid,
          signalDbm = None,
          detailsJson = TidbTransactor.jsonDetails(
            "attack_chain" -> TidbTransactor.parsedJson(row.attackChain),
            "first_event_at" -> row.firstEventAt.withOffsetSameInstant(ZoneOffset.UTC).toString,
            "last_event_at" -> row.lastEventAt.withOffsetSameInstant(ZoneOffset.UTC).toString,
            "factor_breakdown" -> TidbTransactor.parsedJson(row.factorBreakdown),
            "explanation" -> TidbTransactor.parsedJson(row.explanation)
          ),
          rawJson = row.rawJson
        )
    })

  override def insertWirelessSequenceAlert(batchId: String, rows: List[WirelessSequenceAlertInsert]): IO[Long] =
    mergeWirelessAlerts(batchId, "sequence_alert", rows.map { row =>
        WirelessAlertRow(
          rowSequence = row.rowSequence,
          detectedAt = row.detectedAt,
          sensorId = row.sensorId,
          locationId = row.locationId,
          iface = None,
          channel = None,
          primaryMac = row.sourceMac,
          secondaryMac = row.bssid,
          ssid = row.ssid,
          signalDbm = None,
          detailsJson = TidbTransactor.jsonDetails(
            "session_key" -> row.sessionKey,
            "attack_tag" -> row.attackTag,
            "sequence" -> TidbTransactor.parsedJson(row.sequence),
            "first_event_at" -> row.firstEventAt.withOffsetSameInstant(ZoneOffset.UTC).toString,
            "last_event_at" -> row.lastEventAt.withOffsetSameInstant(ZoneOffset.UTC).toString,
            "factor_breakdown" -> TidbTransactor.parsedJson(row.factorBreakdown),
            "explanation" -> TidbTransactor.parsedJson(row.explanation)
          ),
          rawJson = row.rawJson
        )
    })

  override def insertWirelessHandshakeAlert(batchId: String, rows: List[WirelessHandshakeAlertInsert]): IO[Long] =
    mergeWirelessAlerts(batchId, "handshake", rows.map { row =>
        WirelessAlertRow(
          rowSequence = row.rowSequence,
          detectedAt = row.detectedAt,
          sensorId = row.sensorId,
          locationId = row.locationId,
          iface = Some(row.iface),
          channel = None,
          primaryMac = Some(row.clientMac),
          secondaryMac = Some(row.bssid),
          ssid = None,
          signalDbm = row.signalDbm,
          detailsJson = TidbTransactor.jsonDetails("pmkid_sha256" -> row.pmkidSha256),
          rawJson = None
        )
    })

  private def mergeWirelessAlerts(
      batchId: String,
      alertType: String,
      rows: List[WirelessAlertRow]
  ): IO[Long] =
    val now = Timestamp.from(Instant.now())
    withTransactionRetry(s"merge_wireless_$alertType") { conn =>
      val stmt = conn.prepareStatement(BatchSinkSql.UpsertWirelessAlerts)
      try
        val params = rows.map { row =>
          Seq[Any](
            alertType, batchId, row.rowSequence, ts(row.detectedAt), row.sensorId,
            row.locationId, optStr(row.iface), optLong(row.channel),
            optStr(row.primaryMac), optStr(row.secondaryMac), optStr(row.ssid),
            optLong(row.signalDbm), row.detailsJson, optStr(row.rawJson), now, now
          )
        }
        executeBatch(stmt, params)
      finally stmt.close()
    }

  private def bandForChannel(channel: Long): String =
    if channel >= 1 && channel <= 14 then "2.4GHz" else "5GHz"

  // ── wireless_client_inventory ─────────────────────────────────
  override def insertWirelessClientInventory(batchId: String, rows: List[WirelessClientInventoryInsert]): IO[Long] =
    withTransactionRetry("insert_wireless_client_inventory") { conn =>
      val stmt = conn.prepareStatement(BatchSinkSql.UpsertWirelessClientInventory)
      try
        val params = rows.map(r =>
          Seq[Any](
            r.sensorId, r.locationId, ts(r.snapshotAt),
            r.clientMac, optStr(r.bssid), optStr(r.ssid),
            optStr(r.deviceId), optStr(r.username), optStr(r.identitySource),
            ts(r.lastSeen), ts(r.firstSeen),
            optLong(r.signalDbm), r.isAuthorized,
            ts(r.snapshotAt)
          )
        )
        executeBatch(stmt, params)
      finally stmt.close()
    }

  // ── wireless_probe_requests ───────────────────────────────────
  override def insertWirelessProbeRequests(batchId: String, rows: List[WirelessProbeRequestInsert]): IO[Long] =
    withTransactionRetry("insert_wireless_probe_requests") { conn =>
      val stmt = conn.prepareStatement(BatchSinkSql.InsertWirelessProbeRequests)
      try
        val params = rows.map(r =>
          Seq[Any](
            batchId, r.rowSequence,
            r.clientMac, r.ssid, optStr(r.knownBssid),
            ts(r.firstSeen), ts(r.lastSeen),
            r.probeCount, ts(r.firstSeen)
          )
        )
        executeBatch(stmt, params)
      finally stmt.close()
    }

  def preflightCheck(requiredTables: List[String]): IO[List[String]] =
    if requiredTables.isEmpty then IO.pure(List.empty)
    else withConnection { conn =>
      val sql = SchemaChecksSql.tableLookup(requiredTables.size).fold(
        error => throw IllegalArgumentException(error),
        identity
      )

      val stmt = conn.prepareStatement(sql)
      try
        stmt.setString(1, config.database)
        for (i <- requiredTables.indices) do
          stmt.setString(i + 2, requiredTables(i))
        val rs = stmt.executeQuery()
        val found = scala.collection.mutable.Set.empty[String]
        while rs.next() do
          found += rs.getString("TABLE_NAME").toLowerCase(java.util.Locale.ROOT)
        requiredTables.filterNot(t => found.contains(t.toLowerCase(java.util.Locale.ROOT)))
      finally stmt.close()
    }

  def preflightCheckColumnTypes(
      requiredColumns: List[((String, String), String)]
  ): IO[List[String]] =
    if requiredColumns.isEmpty then IO.pure(List.empty)
    else withConnection { conn =>
      val sql = SchemaChecksSql.columnLookup(requiredColumns.size).fold(
        error => throw IllegalArgumentException(error),
        identity
      )

      val stmt = conn.prepareStatement(sql)
      try
        stmt.setString(1, config.database)
        requiredColumns.zipWithIndex.foreach { case (((table, column), _), index) =>
          stmt.setString(index * 2 + 2, table)
          stmt.setString(index * 2 + 3, column)
        }
        val rs = stmt.executeQuery()
        val found = scala.collection.mutable.Map.empty[(String, String), String]
        while rs.next() do
          val key = (
            rs.getString("TABLE_NAME").toLowerCase(java.util.Locale.ROOT),
            rs.getString("COLUMN_NAME").toLowerCase(java.util.Locale.ROOT)
          )
          found += key -> rs.getString("DATA_TYPE").toLowerCase(java.util.Locale.ROOT)

        requiredColumns.collect {
          case ((table, column), expected)
              if found.get((
                table.toLowerCase(java.util.Locale.ROOT),
                column.toLowerCase(java.util.Locale.ROOT)
              )).forall(_ != expected.toLowerCase(java.util.Locale.ROOT)) =>
            val actual = found.getOrElse(
              (
                table.toLowerCase(java.util.Locale.ROOT),
                column.toLowerCase(java.util.Locale.ROOT)
              ),
              "missing"
            )
            s"$table.$column (expected $expected, found $actual)"
        }
      finally stmt.close()
    }

  def close(): IO[Unit] =
    IO.blocking {
      try ds.close()
      finally tlsMaterial.foreach(_.delete())
      log.info("tidb_pool_closed")
    }

  private def rollbackQuietly(conn: Connection): Unit =
    try conn.rollback()
    catch case _: Exception => ()

object TidbTransactor:
  private val log = StructuredLogger(getClass)

  private[tidb] def jsonDetails(keyValues: (String, Any)*): String =
    val fields = keyValues.flatMap { case (key, value) =>
      unwrapJsonValue(value).map(key -> _)
    }
    Json.obj(fields*).noSpaces

  private def unwrapJsonValue(value: Any): Option[Json] =
    value match
      case null => None
      case None => None
      case Some(inner) => unwrapJsonValue(inner)
      case json: Json => Some(json)
      case number: (java.lang.Byte | java.lang.Short | java.lang.Integer | java.lang.Long) =>
        Some(Json.fromLong(number.longValue))
      case number: java.lang.Number =>
        Some(Json.fromDoubleOrString(number.doubleValue))
      case string: String => Some(Json.fromString(string))
      case boolean: Boolean => Some(Json.fromBoolean(boolean))
      case other => Some(Json.fromString(other.toString))

  private[tidb] def parsedJson(value: Option[String]): Option[Json] =
    value.flatMap(circeParser.parse(_).toOption)

  private final case class WirelessAlertRow(
      rowSequence: Long,
      detectedAt: OffsetDateTime,
      sensorId: String,
      locationId: String,
      iface: Option[String],
      channel: Option[Long],
      primaryMac: Option[String],
      secondaryMac: Option[String],
      ssid: Option[String],
      signalDbm: Option[Long],
      detailsJson: String,
      rawJson: Option[String]
  )

  /** Validate JDBC batch results while the sink reports submitted input rows. */
  private[tidb] def validateBatchResults(results: Array[Int]): Unit =
    import java.sql.Statement

    if results.contains(Statement.EXECUTE_FAILED) then
      throw new BatchUpdateException("JDBC batch reported EXECUTE_FAILED", results)
    else
      results.foreach { affected =>
        if affected < 0 && affected != Statement.SUCCESS_NO_INFO then
          throw new SQLException(
            s"JDBC batch returned an unsupported negative update count: $affected"
          )
      }

  def resource(config: TiDbConfig): Resource[IO, TidbTransactor] =
    Resource.make(allocate(config))(_.close())

  def fromDataSource(ds: HikariDataSource, config: TiDbConfig): TidbTransactor =
    new TidbTransactor(ds, config, None)

  private def allocate(config: TiDbConfig): IO[TidbTransactor] =
    val maxRetries = 10
    val baseDelay: FiniteDuration = 3.seconds

    def tryAllocate: IO[TidbTransactor] =
      IO.blocking {
        val hikariConfig = new HikariConfig()
        hikariConfig.setJdbcUrl(jdbcUrl(config))
        hikariConfig.setUsername(config.user)
        hikariConfig.setPassword(config.password)
        hikariConfig.setMaximumPoolSize(config.poolSize)
        hikariConfig.setConnectionTimeout(config.connectionTimeoutMs)
        hikariConfig.setDriverClassName("com.mysql.cj.jdbc.Driver")
        hikariConfig.setPoolName("tidb-pool")
        hikariConfig.setAutoCommit(true)
        hikariConfig.setConnectionTestQuery(BatchSinkSql.ConnectivityQuery)
        hikariConfig.addDataSourceProperty("cachePrepStmts", "true")
        hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250")
        hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
        hikariConfig.addDataSourceProperty("connectionTimeZone", "UTC")
        hikariConfig.addDataSourceProperty("forceConnectionTimeZoneToSession", "true")
        val tlsMaterial =
          if config.sslMode == "DISABLED" then None
          else Some(TidbTls.configure(hikariConfig, config))

        try
          val ds = new HikariDataSource(hikariConfig)
          log.info("tidb_pool_allocated",
            "host" -> config.host, "port" -> config.port.toString, "database" -> config.database)
          new TidbTransactor(ds, config, tlsMaterial)
        catch
          case error: Throwable =>
            tlsMaterial.foreach(_.delete())
            throw error
      }

    def retryWithBackoff(remaining: Int, lastError: Throwable): IO[TidbTransactor] =
      if remaining <= 0 then
        IO.raiseError(new RuntimeException(
          s"TidbTransactor: failed to allocate pool after $maxRetries attempts", lastError))
      else
        tryAllocate.handleErrorWith { error =>
          val attemptNum = maxRetries - remaining + 1
          val delay = baseDelay * math.min(attemptNum, 5).toLong
          log.warn("tidb_pool_retry",
            "attempt" -> s"$attemptNum/$maxRetries",
            "error" -> Option(error.getMessage).getOrElse(error.getClass.getSimpleName),
            "delay" -> s"${delay.toSeconds}s")
          IO.sleep(delay) *> retryWithBackoff(remaining - 1, error)
        }

    retryWithBackoff(maxRetries, new RuntimeException("no attempts made"))

  def jdbcUrl(config: TiDbConfig): String =
    val base = s"jdbc:mysql://${config.host}:${config.port}/${config.database}" +
      "?rewriteBatchedStatements=true&connectionTimeZone=UTC&forceConnectionTimeZoneToSession=true"
    config.sslMode match
      case "DISABLED" =>
        val publicKeyRetrieval =
          if config.localDevAllowPublicKeyRetrieval then "&allowPublicKeyRetrieval=true"
          else ""
        s"$base&useSSL=false$publicKeyRetrieval"
      case _ =>
        s"$base&sslMode=VERIFY_IDENTITY&fallbackToSystemTrustStore=false"
