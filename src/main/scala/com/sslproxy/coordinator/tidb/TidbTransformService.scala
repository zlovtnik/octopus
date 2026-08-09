package com.sslproxy.coordinator.tidb

import com.sslproxy.coordinator.util.Sha256Utils
import io.circe.Json
import java.nio.charset.StandardCharsets
import java.util.UUID
import JsonFields.*

/** Pure JSON → case class transforms. */
object TidbTransformService:

  // proxy_blocked_host_rollups.risk_score is DECIMAL(10,4). Keep an extreme
  // source score from rolling back the proxy event that owns the rollup.
  private val MinRollupRiskScore = -999999.9999d
  private val MaxRollupRiskScore = 999999.9999d

  def transform(target: TidbSinkTarget, rows: List[Json]): TidbRowSet =
    target match
      case TidbSinkTarget.ProxyEvents             => transformProxyRows(rows)
      case TidbSinkTarget.ProxyPayloadAudit       => TidbRowSet.empty.copy(proxyPayloadAudit = transformProxyPayloadAudit(rows))
      case TidbSinkTarget.WirelessAuditFrames     => TidbRowSet.empty.copy(wirelessAuditFrames = transformWirelessAudit(rows))
      case TidbSinkTarget.WirelessBandwidth       => TidbRowSet.empty.copy(wirelessBandwidth = transformWirelessBandwidth(rows))
      case TidbSinkTarget.WirelessRogueAp         => TidbRowSet.empty.copy(wirelessRogueAp = transformRogueAp(rows))
      case TidbSinkTarget.WirelessDeauthFlood     => TidbRowSet.empty.copy(wirelessDeauthFlood = transformDeauthFlood(rows))
      case TidbSinkTarget.WirelessSignalAnomaly   => TidbRowSet.empty.copy(wirelessSignalAnomaly = transformSignalAnomaly(rows))
      case TidbSinkTarget.WirelessPmfAttack       => TidbRowSet.empty.copy(wirelessPmfAttack = transformPmfAttack(rows))
      case TidbSinkTarget.WirelessClientInventory => TidbRowSet.empty.copy(wirelessClientInventory = transformClientInventory(rows))
      case TidbSinkTarget.WirelessProbeRequests   => TidbRowSet.empty.copy(wirelessProbeRequests = transformProbeRequests(rows))
      case TidbSinkTarget.WirelessAttackSequence  => TidbRowSet.empty.copy(wirelessAttackSequence = transformAttackSequence(rows))
      case TidbSinkTarget.WirelessSequenceAlert   => TidbRowSet.empty.copy(wirelessSequenceAlert = transformSequenceAlert(rows))
      case TidbSinkTarget.WirelessHandshakeAlert  => TidbRowSet.empty.copy(wirelessHandshakeAlert = transformHandshakeAlert(rows))

  private def transformProxyRows(rows: List[Json]): TidbRowSet =
    val proxyRows = List.newBuilder[ProxyEventInsert]
    val blockedRows = List.newBuilder[BlockedEventInsert]
    rows.zipWithIndex.foreach { case (row, index) =>
      val proxyRow = proxyEvent(row)
      proxyRows += proxyRow
      blockedEvent(index, row, proxyRow).foreach(blockedRows += _)
    }
    TidbRowSet.empty.copy(
      proxyEvents = proxyRows.result(),
      blockedEvents = blockedRows.result()
    )

  private def proxyEvent(row: Json): ProxyEventInsert =
    val eventType = requiredString(row, "type", "proxy.events")
    val host = requiredString(row, "host", "proxy.events")
    ProxyEventInsert(
      eventTime = requiredTimestamp(row, "time", "proxy.events"),
      eventType = eventType,
      host = host,
      peerIp = optionalString(row, "peer_ip"),
      wgPubkey = optionalString(row, "wg_pubkey"),
      deviceId = optionalString(row, "device_id"),
      identitySource = optionalString(row, "identity_source").getOrElse("unknown"),
      peerHostname = optionalString(row, "peer_hostname"),
      clientUa = optionalString(row, "client_ua"),
      bytesUp = optionalLong(row, "bytes_up").getOrElse(0L),
      bytesDown = optionalLong(row, "bytes_down").getOrElse(0L),
      statusCode = optionalLong(row, "status_code"),
      blocked = boolFlag(row, "blocked"),
      obfuscationProfile = optionalString(row, "obfuscation_profile"),
      correlationId = optionalString(row, "correlation_id"),
      parentEventId = optionalString(row, "parent_event_id"),
      eventSequence = optionalLong(row, "event_sequence"),
      durationMs = optionalLong(row, "duration_ms"),
      reason = optionalString(row, "reason"),
      rawJson = rawJson(row)
    )

  private def transformProxyPayloadAudit(rows: List[Json]): List[ProxyPayloadAuditInsert] =
    rows.map { row =>
      val raw = rawJson(row).getOrElse("{}")
      ProxyPayloadAuditInsert(
        correlationId = optionalString(row, "correlation_id").getOrElse(stableCorrelationId(raw)),
        host = requiredString(row, "host", "proxy.payload_audit"),
        direction = optionalString(row, "direction").getOrElse("UP").toUpperCase(java.util.Locale.ROOT),
        capturedAt = requiredTimestamp(row, "observed_at", "proxy.payload_audit"),
        byteOffset = optionalLong(row, "byte_offset").getOrElse(0L),
        payloadObjectKey = optionalString(row, "payload_object_key"),
        contentType = optionalString(row, "content_type"),
        httpMethod = optionalString(row, "method"),
        httpStatus = optionalLong(row, "http_status").orElse(optionalLong(row, "status_code")),
        httpPath = optionalString(row, "path"),
        isEncrypted = boolFlag(row, "is_encrypted"),
        truncated = boolFlag(row, "truncated"),
        peerIp = optionalString(row, "peer_ip"),
        notes = optionalString(row, "notes")
      )
    }

  private def blockedEvent(index: Int, row: Json, proxyRow: ProxyEventInsert): Option[BlockedEventInsert] =
    if boolFlag(row, "blocked") == 0L then None
    else
      val blockedBytes = optionalLong(row, "blocked_bytes")
        .orElse(nestedLong(row, "metrics", "blocked_bytes"))
        .orElse(nestedLong(row, "metrics", "total_blocked_bytes_approx"))
        .getOrElse(Math.addExact(proxyRow.bytesUp, proxyRow.bytesDown))
      val verdict = optionalString(row, "verdict").getOrElse("BLOCKED")
      val fingerprint = row.hcursor.downField("fingerprint").focus
      Some(
        BlockedEventInsert(
          rowSequence = rowSequence(index, "proxy blocked rollup"),
          host = proxyRow.host,
          blockedBytes = blockedBytes,
          frequencyHz = optionalDouble(row, "frequency_hz").orElse(nestedDouble(row, "metrics", "frequency_hz")),
          riskScore = boundedRollupRiskScore(
            optionalDouble(row, "risk_score").orElse(nestedDouble(row, "metrics", "risk_score"))
          ),
          category = optionalString(row, "category"),
          verdict = verdict,
          tarpitHeldMs = optionalLong(row, "tarpit_held_ms").getOrElse(0L),
          iatMs = optionalLong(row, "iat_ms").orElse(nestedLong(row, "metrics", "iat_ms")),
          consecutiveBlocks = optionalLong(row, "consecutive_blocks")
            .orElse(nestedLong(row, "metrics", "consecutive_blocks"))
            .orElse(optionalLong(row, "attempt_count"))
            .orElse(nestedLong(row, "metrics", "attempt_count")),
          lastVerdict = optionalString(row, "last_verdict"),
          tlsVer = nestedString(fingerprint, "tls_ver"),
          alpn = nestedString(fingerprint, "alpn"),
          ja3Lite = nestedString(fingerprint, "ja3_lite"),
          resolvedIp = optionalString(row, "resolved_ip"),
          asnOrg = optionalString(row, "asn_org")
        )
      )

  private def boundedRollupRiskScore(score: Option[Double]): Option[Double] =
    score.map(value => Math.max(MinRollupRiskScore, Math.min(MaxRollupRiskScore, value)))

  private def transformWirelessAudit(rows: List[Json]): List[WirelessAuditFrameInsert] =
    rows.zipWithIndex.map { case (row, index) =>
      WirelessAuditFrameInsert(
        rowSequence = rowSequence(index, "wireless.audit"),
        eventType = requiredString(row, "event_type", "wireless.audit"),
        observedAt = requiredTimestamp(row, "observed_at", "wireless.audit"),
        sensorId = requiredString(row, "sensor_id", "wireless.audit"),
        locationId = requiredString(row, "location_id", "wireless.audit"),
        iface = requiredString(row, "interface", "wireless.audit"),
        channel = requiredLong(row, "channel", "wireless.audit"),
        frameType = optionalString(row, "frame_type"),
        frameSubtype = requiredString(row, "frame_subtype", "wireless.audit"),
        bssid = optionalString(row, "bssid"),
        sourceMac = optionalString(row, "source_mac"),
        destinationMac = optionalString(row, "destination_mac"),
        transmitterMac = optionalString(row, "transmitter_mac"),
        receiverMac = optionalString(row, "receiver_mac"),
        destinationBssid = optionalString(row, "destination_bssid"),
        ssid = row.hcursor.get[String]("ssid").toOption,
        signalDbm = optionalLong(row, "signal_dbm"),
        sequenceNumber = optionalLong(row, "sequence_number"),
        rawLen = requiredLong(row, "raw_len", "wireless.audit"),
        isRetry = boolFlag(row, "retry"),
        isMoreData = boolFlag(row, "more_data"),
        isPowerSave = boolFlag(row, "power_save"),
        isProtected = boolFlag(row, "protected"),
        isToDs = boolFlag(row, "to_ds"),
        isFromDs = boolFlag(row, "from_ds"),
        isHandshake = boolFlag(row, "handshake_captured"),
        securityFlags = optionalLong(row, "security_flags").getOrElse(0L),
        deviceId = optionalString(row, "device_id"),
        username = optionalString(row, "username"),
        identitySource = optionalString(row, "identity_source").getOrElse("unknown"),
        tags = jsonArrayString(row, "tags"),
        anomalyReasons = jsonArrayString(row, "anomaly_reasons"),
        rawJson = rawJson(row),
        regDomain = optionalString(row, "reg_domain")
      )
    }

  private def transformWirelessBandwidth(rows: List[Json]): List[WirelessBandwidthInsert] =
    rows.zipWithIndex.map { case (row, index) =>
      WirelessBandwidthInsert(
        rowSequence = rowSequence(index, "audit.wireless.bandwidth"),
        schemaVersion = optionalLong(row, "schema_version").getOrElse(1L),
        windowStart = requiredTimestamp(row, "window_start", "audit.wireless.bandwidth"),
        windowEnd = requiredTimestamp(row, "window_end", "audit.wireless.bandwidth"),
        sensorId = requiredString(row, "sensor_id", "audit.wireless.bandwidth"),
        locationId = requiredString(row, "location_id", "audit.wireless.bandwidth"),
        iface = requiredString(row, "interface", "audit.wireless.bandwidth"),
        channel = requiredLong(row, "channel", "audit.wireless.bandwidth"),
        sourceMac = requiredString(row, "source_mac", "audit.wireless.bandwidth"),
        destinationBssid = requiredString(row, "destination_bssid", "audit.wireless.bandwidth"),
        ssid = optionalString(row, "ssid"),
        bytes = requiredLong(row, "bytes", "audit.wireless.bandwidth"),
        frameCount = requiredLong(row, "frame_count", "audit.wireless.bandwidth"),
        retryCount = optionalLong(row, "retry_count").getOrElse(0L),
        moreDataCount = optionalLong(row, "more_data_count").getOrElse(0L),
        powerSaveCount = optionalLong(row, "power_save_count").getOrElse(0L),
        strongestSignalDbm = optionalLong(row, "strongest_signal_dbm"),
        histUnder100 = nestedLong(row, "frame_size_histogram", "under_100").getOrElse(0L),
        hist100500 = nestedLong(row, "frame_size_histogram", "range_100_500").getOrElse(0L),
        hist5001000 = nestedLong(row, "frame_size_histogram", "range_500_1000").getOrElse(0L),
        hist10001500 = nestedLong(row, "frame_size_histogram", "range_1000_1500").getOrElse(0L),
        interArrivalP50Ms = optionalLong(row, "inter_arrival_p50_ms"),
        externalBssid = boolFlag(row, "external_bssid"),
        thresholdExceeded = boolFlag(row, "threshold_exceeded"),
        wallClockDeltaMs = optionalLong(row, "wall_clock_delta_ms"),
        windowIsPartial = boolFlag(row, "window_is_partial"),
        publishedAt = optionalTimestamp(row, "published_at")
      )
    }

  private def transformRogueAp(rows: List[Json]): List[WirelessRogueApInsert] =
    rows.zipWithIndex.map { case (row, index) =>
      val ssidImpersonation =
        boolFlag(row, "ssid_impersonation") | reasonFlag(row, "ssid_impersonation", "bssid_spoofing")
      WirelessRogueApInsert(
        rowSequence = rowSequence(index, "wireless.alert.rogue_ap"),
        detectedAt = timestampAlias(row, "detected_at", "observed_at", "wireless.alert.rogue_ap"),
        sensorId = requiredString(row, "sensor_id", "wireless.alert.rogue_ap"),
        locationId = requiredString(row, "location_id", "wireless.alert.rogue_ap"),
        iface = requiredString(row, "interface", "wireless.alert.rogue_ap"),
        channel = requiredLong(row, "channel", "wireless.alert.rogue_ap"),
        rogueBssid = stringAlias(row, "rogue_bssid", "bssid", "wireless.alert.rogue_ap"),
        ssid = optionalString(row, "ssid"),
        signalDbm = optionalLong(row, "signal_dbm"),
        ssidImpersonation = ssidImpersonation,
        rawJson = rawJson(row)
      )
    }

  private def transformDeauthFlood(rows: List[Json]): List[WirelessDeauthFloodInsert] =
    rows.zipWithIndex.map { case (row, index) =>
      WirelessDeauthFloodInsert(
        rowSequence = rowSequence(index, "wireless.alert.deauth_flood"),
        detectedAt = timestampAlias(row, "detected_at", "observed_at", "wireless.alert.deauth_flood"),
        sensorId = requiredString(row, "sensor_id", "wireless.alert.deauth_flood"),
        locationId = requiredString(row, "location_id", "wireless.alert.deauth_flood"),
        iface = requiredString(row, "interface", "wireless.alert.deauth_flood"),
        channel = optionalLong(row, "channel").getOrElse(0L),
        attackerMac = optionalString(row, "attacker_mac").orElse(optionalString(row, "source_mac")),
        targetBssid = optionalString(row, "target_bssid").orElse(optionalString(row, "bssid")),
        targetSsid = optionalString(row, "target_ssid").orElse(optionalString(row, "ssid")),
        deauthCount = longAlias(row, "deauth_count", "frame_count", "wireless.alert.deauth_flood"),
        windowSecs = requiredLong(row, "window_secs", "wireless.alert.deauth_flood"),
        threshold = optionalLong(row, "threshold").getOrElse(0L),
        signalDbm = optionalLong(row, "signal_dbm"),
        rawJson = rawJson(row)
      )
    }

  private def transformSignalAnomaly(rows: List[Json]): List[WirelessSignalAnomalyInsert] =
    rows.zipWithIndex.map { case (row, index) =>
      WirelessSignalAnomalyInsert(
        rowSequence = rowSequence(index, "wireless.alert.signal_anomaly"),
        detectedAt = timestampAlias(row, "detected_at", "observed_at", "wireless.alert.signal_anomaly"),
        sensorId = requiredString(row, "sensor_id", "wireless.alert.signal_anomaly"),
        locationId = requiredString(row, "location_id", "wireless.alert.signal_anomaly"),
        sourceMac = requiredString(row, "source_mac", "wireless.alert.signal_anomaly"),
        bssid = optionalString(row, "bssid"),
        ssid = optionalString(row, "ssid"),
        channel = requiredLong(row, "channel", "wireless.alert.signal_anomaly"),
        baselineDbm = requiredLong(row, "baseline_dbm", "wireless.alert.signal_anomaly"),
        observedDbm = requiredLong(row, "observed_dbm", "wireless.alert.signal_anomaly"),
        dbmDelta = Math.abs(requiredLong(row, "dbm_delta", "wireless.alert.signal_anomaly")),
        configuredDelta = requiredLong(row, "configured_delta", "wireless.alert.signal_anomaly")
      )
    }

  private def transformPmfAttack(rows: List[Json]): List[WirelessPmfAttackInsert] =
    rows.zipWithIndex.map { case (row, index) =>
      WirelessPmfAttackInsert(
        rowSequence = rowSequence(index, "wireless.alert.pmf_attack"),
        detectedAt = timestampAlias(row, "detected_at", "observed_at", "wireless.alert.pmf_attack"),
        sensorId = requiredString(row, "sensor_id", "wireless.alert.pmf_attack"),
        locationId = requiredString(row, "location_id", "wireless.alert.pmf_attack"),
        targetMac = stringAlias(row, "target_mac", "source_mac", "wireless.alert.pmf_attack"),
        targetBssid = optionalString(row, "target_bssid").orElse(optionalString(row, "bssid")),
        ssid = optionalString(row, "ssid"),
        channel = optionalLong(row, "channel"),
        attackTag = requiredString(row, "attack_tag", "wireless.alert.pmf_attack"),
        reconnectWindowMs = optionalLong(row, "reconnect_window_ms")
      )
    }

  private def transformClientInventory(rows: List[Json]): List[WirelessClientInventoryInsert] =
    rows.map { row =>
      WirelessClientInventoryInsert(
        sensorId = requiredString(row, "sensor_id", "wireless.client.inventory"),
        locationId = requiredString(row, "location_id", "wireless.client.inventory"),
        snapshotAt = requiredTimestamp(row, "snapshot_at", "wireless.client.inventory"),
        clientMac = stringAlias(row, "client_mac", "source_mac", "wireless.client.inventory"),
        bssid = optionalString(row, "bssid"),
        ssid = optionalString(row, "ssid"),
        deviceId = optionalString(row, "device_id"),
        username = optionalString(row, "username"),
        identitySource = optionalString(row, "identity_source"),
        lastSeen = requiredTimestamp(row, "last_seen", "wireless.client.inventory"),
        firstSeen = requiredTimestamp(row, "first_seen", "wireless.client.inventory"),
        signalDbm = optionalLong(row, "signal_dbm").orElse(optionalLong(row, "last_signal_dbm")),
        isAuthorized = boolFlag(row, "is_authorized")
      )
    }

  private def transformProbeRequests(rows: List[Json]): List[WirelessProbeRequestInsert] =
    rows.zipWithIndex.map { case (row, index) =>
      WirelessProbeRequestInsert(
        rowSequence = rowSequence(index, "wireless.probe.flush"),
        clientMac = requiredString(row, "client_mac", "wireless.probe.flush"),
        ssid = requiredString(row, "ssid", "wireless.probe.flush"),
        knownBssid = optionalString(row, "known_bssid"),
        firstSeen = requiredTimestamp(row, "first_seen", "wireless.probe.flush"),
        lastSeen = requiredTimestamp(row, "last_seen", "wireless.probe.flush"),
        probeCount = requiredLong(row, "probe_count", "wireless.probe.flush")
      )
    }

  private def transformAttackSequence(rows: List[Json]): List[WirelessAttackSequenceInsert] =
    rows.zipWithIndex.map { case (row, index) =>
      WirelessAttackSequenceInsert(
        rowSequence = rowSequence(index, "wireless.alert.attack_sequence"),
        detectedAt = timestampAlias(row, "detected_at", "observed_at", "wireless.alert.attack_sequence"),
        sensorId = requiredString(row, "sensor_id", "wireless.alert.attack_sequence"),
        locationId = requiredString(row, "location_id", "wireless.alert.attack_sequence"),
        ssid = presentString(row, "ssid"),
        attackChain = jsonArrayString(row, "attack_chain"),
        firstEventAt = requiredTimestamp(row, "first_event_at", "wireless.alert.attack_sequence"),
        lastEventAt = requiredTimestamp(row, "last_event_at", "wireless.alert.attack_sequence"),
        factorBreakdown = jsonArrayString(row, "factor_breakdown"),
        explanation = jsonArrayString(row, "explanation"),
        rawJson = rawJson(row)
      )
    }

  private def transformSequenceAlert(rows: List[Json]): List[WirelessSequenceAlertInsert] =
    rows.zipWithIndex.map { case (row, index) =>
      WirelessSequenceAlertInsert(
        rowSequence = rowSequence(index, "wireless.alert.sequence"),
        detectedAt = timestampAlias(row, "detected_at", "observed_at", "wireless.alert.sequence"),
        sensorId = requiredString(row, "sensor_id", "wireless.alert.sequence"),
        locationId = requiredString(row, "location_id", "wireless.alert.sequence"),
        sessionKey = requiredString(row, "session_key", "wireless.alert.sequence"),
        sourceMac = optionalString(row, "source_mac"),
        bssid = optionalString(row, "bssid"),
        ssid = presentString(row, "ssid"),
        attackTag = requiredString(row, "attack_tag", "wireless.alert.sequence"),
        sequence = jsonArrayString(row, "sequence"),
        firstEventAt = requiredTimestamp(row, "first_event_at", "wireless.alert.sequence"),
        lastEventAt = requiredTimestamp(row, "last_event_at", "wireless.alert.sequence"),
        factorBreakdown = jsonArrayString(row, "factor_breakdown"),
        explanation = jsonArrayString(row, "explanation"),
        rawJson = rawJson(row)
      )
    }

  private def transformHandshakeAlert(rows: List[Json]): List[WirelessHandshakeAlertInsert] =
    rows.zipWithIndex.map { case (row, index) =>
      WirelessHandshakeAlertInsert(
        rowSequence = rowSequence(index, "wireless.alert.handshake"),
        detectedAt = timestampAlias(row, "detected_at", "observed_at", "wireless.alert.handshake"),
        sensorId = requiredString(row, "sensor_id", "wireless.alert.handshake"),
        locationId = requiredString(row, "location_id", "wireless.alert.handshake"),
        iface = requiredString(row, "interface", "wireless.alert.handshake"),
        bssid = requiredString(row, "bssid", "wireless.alert.handshake"),
        clientMac = requiredString(row, "client_mac", "wireless.alert.handshake"),
        signalDbm = optionalLong(row, "signal_dbm"),
        pmkidSha256 = optionalString(row, "pmkid").map(Sha256Utils.sha256Hex)
      )
    }

  private def nestedString(parent: Option[Json], field: String): Option[String] =
    parent.flatMap { p =>
      if p.isNull then None
      else optionalString(p, field)
    }

  private def reasonFlag(row: Json, first: String, second: String): Long =
    row.hcursor.downField("reasons").focus match
      case Some(reasons) if reasons.isArray =>
        reasons.asArray.getOrElse(Vector.empty).exists { r =>
          r.asString.exists(s => s == first || s == second)
        } match
          case true  => 1L
          case false => 0L
      case _ => 0L

  private def stableCorrelationId(rawJson: String): String =
    UUID.nameUUIDFromBytes(rawJson.getBytes(StandardCharsets.UTF_8)).toString
